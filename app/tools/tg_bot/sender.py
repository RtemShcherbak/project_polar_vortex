from pathlib import Path
from telegram import Bot
from telegram.error import TelegramError



async def send_image(
    bot: Bot,
    chat_id: int,
    image_path: Path,
    caption: str,
):
    try:
        with image_path.open("rb") as f:
            await bot.send_photo(
                chat_id=chat_id,
                photo=f,
                caption=caption,
            )

    except TelegramError as e:
        raise RuntimeError(
            f"Telegram send failed: chat_id={chat_id}, file={image_path.name}"
        ) from e



async def send_images(
    bot: Bot,
    chat_id: int,
    images: list[Path],
    tag: str,
):
    for img in images:
        await send_image(
            bot=bot,
            chat_id=chat_id,
            image_path=img,
            caption=f"{tag}\n{img.name}",
        )
