import os
import re
import time
import shutil
import asyncio
import logging
from datetime import datetime
from PIL import Image
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pyrogram.types import InputMediaDocument, Message
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
from helper.utils import progress_for_pyrogram, humanbytes, convert
from helper.database import codeflixbots
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global dictionary to track ongoing operations
renaming_operations = {}

# Enhanced regex patterns for season and episode extraction
SEASON_EPISODE_PATTERNS = [
    # Standard patterns (S01E02, S01EP02, S01E02, S01 E02, S01 EP02) - PRIORITY
    (re.compile(r'S(\d+)\s*(?:E|EP)\s*(\d+)', re.IGNORECASE), ('season', 'episode')),
    # Patterns with dashes (S01-E02, S01-EP02)
    (re.compile(r'S(\d+)[-_](?:E|EP)(\d+)', re.IGNORECASE), ('season', 'episode')),
    # Full text patterns (Season 1 Episode 2, Season 01 Episode 02)
    (re.compile(r'Season\s*(\d+)\s*Episode\s*(\d+)', re.IGNORECASE), ('season', 'episode')),
    # Patterns with brackets/parentheses ([S01][E02], [S01E02])
    (re.compile(r'\[S(\d+)\s*(?:E|EP)?\s*(\d+)\]', re.IGNORECASE), ('season', 'episode')),
    (re.compile(r'\[S(\d+)\]\[E(\d+)\]', re.IGNORECASE), ('season', 'episode')),
    # Fallback patterns (S01 13 - season followed by number)
    (re.compile(r'S(\d+)[^\d]*(\d+)', re.IGNORECASE), ('season', 'episode')),
    # Episode only patterns (EP02, E02, Episode 02)
    (re.compile(r'(?:E|EP|Episode)\s*(\d+)', re.IGNORECASE), (None, 'episode')),
]

QUALITY_PATTERNS = [
    # Bracketed quality formats (PRIORITY) - [480p], [720p], [1080p], [2160p]
    (re.compile(r'\[(\d{3,4}[pi])\]', re.IGNORECASE), lambda m: m.group(1).lower()),
    # Standard quality formats - 1080p, 720p, 480p, 2160p
    (re.compile(r'\b(\d{3,4}[pi])\b', re.IGNORECASE), lambda m: m.group(1).lower()),
    # 4K and 2K variants - [4K], 4K, [2160p], 2160p
    (re.compile(r'\[?(4k|2160p|uhd)\]?', re.IGNORECASE), lambda m: "4k"),
    (re.compile(r'\[?(2k|1440p|qhd)\]?', re.IGNORECASE), lambda m: "2k"),
    # HDRip, HDTV, WebRip variants - [HDRip], HDRip
    (re.compile(r'\[?(HDRip|HDTV|WebRip|WEBRip|BluRay|BRRip)\]?', re.IGNORECASE), lambda m: m.group(1).lower()),
    # x264/x265 variants with quality
    (re.compile(r'\[?(4k|2k|1080p|720p|480p)?\s*[xX](264|265)\]?', re.IGNORECASE), lambda m: m.group(0).lower()),
]

def extract_season_episode(caption, filename):
    """Extract season and episode numbers from caption first, then filename"""
    # Try caption first if available
    if caption:
        for pattern, (season_group, episode_group) in SEASON_EPISODE_PATTERNS:
            match = pattern.search(caption)
            if match:
                season = match.group(1) if season_group else None
                episode = match.group(2) if episode_group else match.group(1)
                logger.info(f"Extracted season: {season}, episode: {episode} from caption")
                return season, episode
    
    # Fallback to filename
    for pattern, (season_group, episode_group) in SEASON_EPISODE_PATTERNS:
        match = pattern.search(filename)
        if match:
            season = match.group(1) if season_group else None
            episode = match.group(2) if episode_group else match.group(1)
            logger.info(f"Extracted season: {season}, episode: {episode} from filename")
            return season, episode
    
    logger.warning(f"No season/episode pattern matched for caption or filename")
    return None, None

def extract_quality(caption, filename):
    """Extract quality information from caption first, then filename"""
    # Try caption first if available
    if caption:
        for pattern, extractor in QUALITY_PATTERNS:
            match = pattern.search(caption)
            if match:
                quality = extractor(match)
                logger.info(f"Extracted quality: {quality} from caption")
                return quality
    
    # Fallback to filename
    for pattern, extractor in QUALITY_PATTERNS:
        match = pattern.search(filename)
        if match:
            quality = extractor(match)
            logger.info(f"Extracted quality: {quality} from filename")
            return quality
    
    logger.warning(f"No quality pattern matched for caption or filename")
    return "Unknown"

async def cleanup_files(*paths):
    """Safely remove files if they exist"""
    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logger.error(f"Error removing {path}: {e}")

async def process_thumbnail(thumb_path):
    """Process and resize thumbnail image"""
    if not thumb_path or not os.path.exists(thumb_path):
        return None
    
    try:
        with Image.open(thumb_path) as img:
            img = img.convert("RGB").resize((1280, 720))
            img.save(thumb_path, "JPEG")
        return thumb_path
    except Exception as e:
        logger.error(f"Thumbnail processing failed: {e}")
        await cleanup_files(thumb_path)
        return None

def get_reliable_duration(message, file_size):
    """
    Get reliable duration from Telegram metadata.
    Returns None if duration is unreliable (< 60 seconds) or missing.
    Uses file_size as primary indicator for actual content length.
    
    Args:
        message: Telegram message object
        file_size: File size in bytes
        
    Returns:
        Reliable duration in seconds or None
    """
    duration = None
    
    # Extract duration from media type
    if message.video and hasattr(message.video, 'duration'):
        duration = message.video.duration
    elif message.audio and hasattr(message.audio, 'duration'):
        duration = message.audio.duration
    
    # Don't trust duration if it's too small (< 60 seconds)
    # Telegram sometimes reports 0 or 1 second on first upload
    if duration and duration < 60:
        logger.warning(f"Ignoring unreliable duration: {duration}s (file size: {humanbytes(file_size)})")
        duration = None
    
    # Additional validation: if file is large (> 10MB), duration should exist
    # If it doesn't or is too small, it's unreliable
    if file_size > 10 * 1024 * 1024:  # 10MB
        if not duration or duration < 60:
            logger.warning(f"Large file ({humanbytes(file_size)}) with suspicious duration, treating as unreliable")
            duration = None
    
    if duration:
        logger.info(f"Using reliable duration: {duration}s for file size: {humanbytes(file_size)}")
    else:
        logger.info(f"No reliable duration available, using file_size ({humanbytes(file_size)}) as primary indicator")
    
    return duration

async def add_metadata(input_path, output_path, user_id):
    """Add metadata to media file using ffmpeg"""
    ffmpeg = shutil.which('ffmpeg')
    if not ffmpeg:
        raise RuntimeError("FFmpeg not found in PATH")
    
    metadata = {
        'title': await codeflixbots.get_title(user_id),
        'artist': await codeflixbots.get_artist(user_id),
        'author': await codeflixbots.get_author(user_id),
        'video_title': await codeflixbots.get_video(user_id),
        'audio_title': await codeflixbots.get_audio(user_id),
        'subtitle': await codeflixbots.get_subtitle(user_id)
    }
    
    cmd = [
        ffmpeg,
        '-i', input_path,
        '-metadata', f'title={metadata["title"]}',
        '-metadata', f'artist={metadata["artist"]}',
        '-metadata', f'author={metadata["author"]}',
        '-metadata:s:v', f'title={metadata["video_title"]}',
        '-metadata:s:a', f'title={metadata["audio_title"]}',
        '-metadata:s:s', f'title={metadata["subtitle"]}',
        '-map', '0',
        '-c', 'copy',
        '-loglevel', 'error',
        output_path
    ]
    
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await process.communicate()
    
    if process.returncode != 0:
        raise RuntimeError(f"FFmpeg error: {stderr.decode()}")

@Client.on_message(filters.private & (filters.document | filters.video | filters.audio))
async def auto_rename_files(client, message):
    """Main handler for auto-renaming files"""
    user_id = message.from_user.id
    format_template = await codeflixbots.get_format_template(user_id)
    
    if not format_template:
        return await message.reply_text("Please set a rename format using /autorename")

    # Get file information
    if message.document:
        file_id = message.document.file_id
        file_name = message.document.file_name
        file_size = message.document.file_size
        media_type = "document"
    elif message.video:
        file_id = message.video.file_id
        file_name = message.video.file_name or "video"
        file_size = message.video.file_size
        media_type = "video"
    elif message.audio:
        file_id = message.audio.file_id
        file_name = message.audio.file_name or "audio"
        file_size = message.audio.file_size
        media_type = "audio"
    else:
        return await message.reply_text("Unsupported file type")

    reliable_duration = get_reliable_duration(message, file_size)
    
    # Log file information for debugging
    logger.info(f"Processing file: {file_name}")
    logger.info(f"File size: {humanbytes(file_size)}")
    logger.info(f"Reliable duration: {reliable_duration}s" if reliable_duration else "Duration: unreliable/not available")

    # Prevent duplicate processing
    if file_id in renaming_operations:
        if (datetime.now() - renaming_operations[file_id]).seconds < 10:
            return
    renaming_operations[file_id] = datetime.now()

    download_path = None
    metadata_path = None
    thumb_path = None

    try:
        caption = message.caption if message.caption else None
        
        # Extract metadata from caption first, then filename
        season, episode = extract_season_episode(caption, file_name)
        quality = extract_quality(caption, file_name)
        
        # Replace placeholders in template
        replacements = {
            '{season}': season or 'XX',
            '{episode}': episode or 'XX',
            '{quality}': quality,
            'Season': season or 'XX',
            'Episode': episode or 'XX',
            'QUALITY': quality
        }
        
        for placeholder, value in replacements.items():
            format_template = format_template.replace(placeholder, value)

        # Prepare file paths
        ext = os.path.splitext(file_name)[1] or ('.mp4' if media_type == 'video' else '.mp3')
        new_filename = f"{format_template}{ext}"
        download_path = f"downloads/{new_filename}"
        metadata_path = f"metadata/{new_filename}"
        
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)

        # Download file
        msg = await message.reply_text("**Downloading...**")
        try:
            file_path = await client.download_media(
                message,
                file_name=download_path,
                progress=progress_for_pyrogram,
                progress_args=("Downloading...", msg, time.time())
            )
        except FloodWait as e:
            logger.warning(f"FloodWait: Sleeping for {e.value} seconds")
            await asyncio.sleep(e.value)
            file_path = await client.download_media(message, file_name=download_path)
        except TimeoutError:
            await msg.edit("**Download timeout. Please try again.**")
            raise
        except Exception as e:
            await msg.edit(f"Download failed: {e}")
            raise

        # Process metadata
        await msg.edit("**Processing metadata...**")
        try:
            await add_metadata(file_path, metadata_path, user_id)
            file_path = metadata_path
        except Exception as e:
            logger.warning(f"Metadata processing failed, using original file: {e}")
            # Continue without metadata if processing fails
            file_path = download_path

        # Prepare for upload
        await msg.edit("**Preparing upload...**")
        caption = await codeflixbots.get_caption(message.chat.id) or f"**{new_filename}**"
        thumb = await codeflixbots.get_thumbnail(message.chat.id)
        thumb_path = None

        # Handle thumbnail
        if thumb:
            try:
                thumb_path = await client.download_media(thumb)
            except Exception as e:
                logger.warning(f"Failed to download custom thumbnail: {e}")
        elif media_type == "video" and message.video.thumbs:
            try:
                thumb_path = await client.download_media(message.video.thumbs[0].file_id)
            except Exception as e:
                logger.warning(f"Failed to download video thumbnail: {e}")
        
        thumb_path = await process_thumbnail(thumb_path)

        await msg.edit("**Uploading...**")
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                upload_params = {
                    'chat_id': message.chat.id,
                    'caption': caption,
                    'thumb': thumb_path,
                    'progress': progress_for_pyrogram,
                    'progress_args': ("Uploading...", msg, time.time())
                }

                await client.send_video(
                    video=file_path, 
                    supports_streaming=True,
                    **upload_params
                )

                await msg.delete()
                break  # Success - exit retry loop
                
            except FloodWait as e:
                logger.warning(f"FloodWait during upload: Sleeping for {e.value} seconds")
                await msg.edit(f"**Rate limited. Waiting {e.value} seconds...**")
                await asyncio.sleep(e.value)
                retry_count += 1
                
            except TimeoutError:
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"Upload timeout. Retry {retry_count}/{max_retries}")
                    await msg.edit(f"**Upload timeout. Retrying ({retry_count}/{max_retries})...**")
                    await asyncio.sleep(5)
                else:
                    await msg.edit("**Upload failed after multiple retries. Please try again later.**")
                    raise
                    
            except Exception as e:
                logger.error(f"Upload error: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    await msg.edit(f"**Upload error. Retrying ({retry_count}/{max_retries})...**")
                    await asyncio.sleep(3)
                else:
                    await msg.edit(f"Upload failed: {e}")
                    raise

    except Exception as e:
        logger.error(f"Processing error: {e}")
        try:
            await message.reply_text(f"Error: {str(e)}")
        except:
            pass
    finally:
        # Clean up files
        await cleanup_files(download_path, metadata_path, thumb_path)
        renaming_operations.pop(file_id, None)
        
