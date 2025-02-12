"""This script migrates Instagram posts to Bluesky.

It reads the Instagram archive and extracts posts, then migrates them to Bluesky.

NOTE: the script will not do anything unless you add --migrate to the command line.

RECOMMENDED: use --pick 1 to test with a single post first.
"""

import os
import json
import argparse
from datetime import datetime
from omegaconf import OmegaConf
from loguru import logger
from getpass import getpass

from src.engines import InstagramArchiveParsingEngine, BlueSkyPostingEngine
from src.models import MigrationConfig


def main(args):
    """Main function."""
    logger.info(f"Loading config from {args.config}")
    config = MigrationConfig(**OmegaConf.load(args.config))

    if args.command == "import":
        logger.info(f"Importing posts from Instagram archive: {args.archive_folder}")

        # initialize the engine with cli args
        archive_parsing_engine = InstagramArchiveParsingEngine(
            archive_folder=args.archive_folder, config=config
        )
        archive_parsing_engine.extract_posts_to_queue()
        logger.info(
            f"Found {len(archive_parsing_engine.migration_queue.queue)} posts to migrate."
        )

    elif args.command in ["simulate", "migrate"]:
        simulate = args.command == "simulate"

        if simulate:
            logger.info("--- running in SIMULATE mode, no posts will be imported ---")
        else:
            logger.info("--- running in MIGRATE mode, posts will be imported ---")

            if "BLUESKY_PASSWORD" in os.environ:
                password = os.environ["BLUESKY_PASSWORD"]
            else:
                password = getpass("Enter your Bluesky password: ")

        # initialize the engine with cli args
        post_handler = BlueSkyPostingEngine(
            archive_folder=args.archive_folder,
            config=config,
            username=args.username,
            password=password,
            simulate=simulate,
        )
        if args.pick:
            logger.warning(f"Running a test with --pick {args.pick}")
            post_handler.post(index=args.pick)
        else:
            logger.info("Posting to Bluesky...")
            post_handler.post()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import Instagram posts to Bluesky.")
    parser.add_argument(
        "command",
        type=str,
        choices=["import", "migrate", "simulate"],
        help="Command to run.",
    )
    parser.add_argument(
        "--config",
        required=False,
        default=os.path.join(os.path.dirname(__file__), "config.yaml"),
        help="Path to the config file.",
    )
    parser.add_argument(
        "--archive-folder",
        required=False,
        help="Path to the Instagram archive folder.",
    )
    parser.add_argument(
        "--pick",
        required=False,
        type=int,
        help="Index of the post to pick from the queue.",
    )
    parser.add_argument("--username", required=False, help="Bluesky username.")
    parser.add_argument(
        "--log-level", default="INFO", help="Log level (default: INFO)."
    )
    args = parser.parse_args()

    main(args)
