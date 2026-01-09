import asyncio
from pathlib import Path

from app.tools.tg_bot.client import get_bot
from app.tools.tg_bot.sender import send_images
from app.tools.get_secrets import get_secret


CHAT_ID = int(get_secret("TG_CHAT_ID"))
TAG = "#polar_vortex"
IMAGES_DIR = Path("app/data/images")


async def main():
    bot = get_bot()
    images = sorted(IMAGES_DIR.glob("*.png"))

    await send_images(
        bot=bot,
        chat_id=CHAT_ID,
        images=images,
        tag=TAG,
    )


if __name__ == "__main__":
    asyncio.run(main())
