# Telegram Video Downloader Bot (with ETA + Quality Picker)

A Telegram bot that:
- probes a URL for available formats,
- lets you pick quality (Best, 1080p/720p/…),
- supports audio-only and MP3,
- shows **live progress with ETA**,
- uploads as Video/Document (toggle),
- supports Cancel.

> **Legal:** Use only for content you own or have permission to download. No DRM, no paywalls, no logins. The bot does not attempt to bypass DRM.

## Setup

1. **Install system deps**
   - Linux (Ubuntu/Debian):
     ```bash
     sudo apt-get update
     sudo apt-get install -y python3 python3-pip ffmpeg
     ```
2. **Clone & install**
   ```bash
   git clone <your-repo-url> video-downloader-bot
   cd video-downloader-bot
   cp .env.example .env
   # put your BOT_TOKEN etc. in .env
   pip3 install -r requirements.txt
