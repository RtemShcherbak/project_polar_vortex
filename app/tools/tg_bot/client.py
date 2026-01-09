from telegram import Bot
from app.tools.get_secrets import get_secret
TOKEN = get_secret("TG_TOKEN")


def get_bot() -> Bot:
    return Bot(token=TOKEN)
