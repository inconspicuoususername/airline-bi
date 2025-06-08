#!/usr/bin/env python3

"""
This module contains code required to send messages to a slack channel.
Mainly used for load push notifs
"""

import slack_sdk
import slack_sdk.errors

import constants

client = slack_sdk.WebClient(token=constants.BOT_TOKEN)

def send_message(message):
    if constants.CHANNEL is None:
        raise ValueError("CHANNEL is not set")

    client.chat_postMessage(channel=constants.CHANNEL, text=message)


if __name__ == "__main__":
    send_message("Hello, world!")