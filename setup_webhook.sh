#!/bin/bash

# Setup Telegram webhook for Railway deployment
# Usage: ./setup_webhook.sh

# Get Telegram bot token from environment variable
if [ -z "$TELEGRAM_BOT_TOKEN" ]; then
    echo "Error: TELEGRAM_BOT_TOKEN environment variable is not set"
    echo "Please set it in Railway dashboard or export it locally"
    exit 1
fi

RAILWAY_URL="https://water-bottle-production.up.railway.app"
WEBHOOK_URL="${RAILWAY_URL}/api/v1/telegram/webhook"

echo "Setting up Telegram webhook..."
echo "Bot Token: ${TELEGRAM_BOT_TOKEN:0:10}..."
echo "Webhook URL: ${WEBHOOK_URL}"

# Set the webhook
curl -X POST \
  "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook" \
  -H "Content-Type: application/json" \
  -d "{\"url\": \"${WEBHOOK_URL}\"}"

echo ""
echo "Webhook setup complete!"
echo "Test your bot by sending a message to @One_Look_bot"
