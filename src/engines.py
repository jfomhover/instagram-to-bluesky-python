from typing import Any, Dict, List, Optional
from loguru import logger
from datetime import datetime
from pathlib import Path
import json
import glob
import os
import re
import time

from atproto import Client, models, client_utils

from .models import BlueSkyMigrationJob, InstagramArchive
from .models import MigrationConfig
from .models import BlueSkyMigrationJobState


def decode_utf8(data):
    # from https://github.com/marcomaroni-github/instagram-to-bluesky?tab=readme-ov-file
    try:
        if isinstance(data, str):
            # error in FB encoding, see https://stackoverflow.com/questions/50008296/facebook-json-badly-encoded
            return data.encode("latin1").decode("utf-8")
        elif isinstance(data, list):
            return [decode_utf8(item) for item in data]
        elif isinstance(data, dict):
            return {key: decode_utf8(value) for key, value in data.items()}
        return data
    except Exception as e:
        logger.error(f"Error decoding UTF-8 data: {e}")
        return data


def parse_to_richtext(text: str) -> client_utils.TextBuilder:
    # collect all hashtags and use text_builder.tag
    # collect all mentions and use text_builder.mention
    # collect all links and use text_builder.link
    text_builder = client_utils.TextBuilder()

    # cut text into segments
    segments = re.split(r"(\s+)", text)
    for segment in segments:
        if segment.startswith("#"):
            text_builder.tag(tag=segment, text=segment)
        elif segment.startswith("@"):
            text_builder.text(segment)
        elif segment.startswith("http"):
            text_builder.link(url=segment)
        else:
            text_builder.text(segment)
    return text_builder


class MigrationQueue:
    """A queue of migration jobs written/maintained on disk."""

    def __init__(self, path: str):
        self.path = path
        self.queue = []

        os.makedirs(self.path, exist_ok=True)

    def load(self):
        # load all *.json files within self.path
        # transform into BlueSkyMigrationJob objects
        # and add them to self.queue
        for file in glob.glob(f"{self.path}/*.json"):
            with open(file, "r") as f:
                job = BlueSkyMigrationJob(**json.load(f))
                self.queue.append(job)

        # sort by job_index
        self.queue = sorted(self.queue, key=lambda x: x.job_index)
        logger.info(f"Loaded {len(self.queue)} jobs from {self.path}.")

    def append(self, job: BlueSkyMigrationJob):
        # add a job to the queue
        self.queue.append(job)
        self.save(job)

    def save(self, job):
        # save a job to disk
        with open(f"{self.path}/{job.job_index}.json", "w") as f:
            f.write(json.dumps(job.model_dump(), indent=4))


class InstagramArchiveParsingEngine:
    """Engine to parse the Instagram posts to migrate to Bluesky."""

    def __init__(
        self,
        archive_folder: str,
        config: MigrationConfig,
    ):
        """Initialize the engine.

        Args:
            archive_folder: Path to the Instagram archive folder.
            config: MigrationConfig object.
        """
        self.archive_folder = archive_folder
        self.config = config

        # build a queue
        self.migration_queue = MigrationQueue(self.config.queue_dir)

    def _figure_out_post_title(self, post) -> str:
        """Figure out the post title when empty, based on embeds."""
        if post.title is None:
            for media in post.media:
                if media.title:
                    return media.title
        else:
            return post.title

        return "--"

    def extract_posts_to_queue(self):
        """Extract posts from Instagram archive to queue for migration."""
        logger.info(f"Extracting posts from Instagram archive: {self.archive_folder}")

        # load the posts from the archive
        with open(
            Path(self.archive_folder) / "your_instagram_activity/content/posts_1.json",
            "r",
            encoding="utf-8",
        ) as f:
            # insta_posts = json.load(f)
            insta_posts = decode_utf8(json.load(f))

        # parse the archive into a model
        archive = InstagramArchive(posts=insta_posts)
        logger.info(f"Found {len(archive.posts)} posts in Instagram archive.")

        # figure out the post date if not present
        for index, post in enumerate(archive.posts):
            # rectify the creation timestamp if not present
            if post.creation_timestamp is None:
                post.creation_timestamp = (
                    post.media[0].creation_timestamp
                    if post.media and post.media[0].creation_timestamp
                    else None
                )
            if post.creation_timestamp is None:
                logger.warning(
                    f"Will skip post[{index}] - No date found in post {post}"
                )

            # rectify the title if not present
            post.title = self._figure_out_post_title(post)

            # title is still empty, use date instead
            date = datetime.fromtimestamp(post.creation_timestamp).strftime("%Y-%m-%d")
            post.title += f" (from IG, {date})"

        # sort posts by creation timestamp
        archive.posts = sorted(archive.posts, key=lambda x: x.creation_timestamp)

        for index, post in enumerate(archive.posts):
            if self.config.min_date and post.creation_timestamp < self.config.min_date:
                logger.warning(
                    f"Skipping post[{index}] - Before MIN_DATE: [{post.creation_timestamp.strftime('%Y-%m-%d')}]"
                )
                continue
            if self.config.max_date and post.creation_timestamp > self.config.max_date:
                logger.warning(
                    f"Skipping post[{index}] - After MAX_DATE: [{post.creation_timestamp.strftime('%Y-%m-%d')}]"
                )
                break

            if not post.media:
                logger.warning(f"Skipping post[{index}] - No media")
                continue

            # add the post in the queue
            self.queue_post(post, archive_index=index)

        logger.info(f"Queued {len(self.migration_queue.queue)} posts to migrate.")

    def _partition_media(self, media: List[Dict[str, Any]], strategy: str = "ordered"):
        """Partition media into lists of length self.max_images_per_post.

        Args:
            media: List of media to partition.
            strategy: Partitioning strategy. Can be "ordered" or "videolast" (not implemented yet).
        """
        partitions = []
        current_partition = []
        current_partition_type = None

        if strategy == "videolast":
            images = []
            videos = []
            for entry in media:
                if entry.uri.endswith(".mp4"):
                    videos.append(entry)
                else:
                    images.append(entry)
            media = images + videos

        # iterate over the media and partition them
        for entry in media:
            if current_partition_type is None:
                if entry.uri.endswith(".mp4"):
                    current_partition_type = "video"
                else:
                    current_partition_type = "image"

            if current_partition_type == "video" and entry.uri.endswith(".mp4"):
                # post only X video per post
                if len(current_partition) < self.config.max_videos_per_post:
                    current_partition.append(entry)
                else:
                    # if we reach the limit, add the current partition to the list
                    partitions.append(current_partition)
                    current_partition = [entry]
            elif current_partition_type == "image" and not entry.uri.endswith(".mp4"):
                # post only X image per post
                if len(current_partition) < self.config.max_images_per_post:
                    current_partition.append(entry)
                else:
                    # if we reach the limit, add the current partition to the list
                    partitions.append(current_partition)
                    current_partition = [entry]
            else:
                # if we switch partition type, add the current partition to the list
                partitions.append(current_partition)
                current_partition = [entry]
                current_partition_type = (
                    "video" if entry.uri.endswith(".mp4") else "image"
                )

        # add the last partition if it has any media
        if current_partition:
            partitions.append(current_partition)

        return partitions

    def queue_post(self, post, archive_index: int = None):
        """Process a post from the Instagram archive to add it to the migration queue."""
        # figures out if we need to partition the media
        media_partitions = self._partition_media(post.media, self.config.media_strategy)

        assert len(media_partitions) > 0, "Media partitioning failed"

        if len(media_partitions) > 1:
            partition_types = ", ".join(
                [
                    "+".join(
                        [
                            "video" if entry.uri.endswith(".mp4") else "image"
                            for entry in partition
                        ]
                    )
                    for partition in media_partitions
                ]
            )
            logger.info(
                f"Partitioning post[{archive_index}] with {len(post.media)} media into {len(media_partitions)} posts: {partition_types})."
            )

        thread_start = None
        reply_to = None

        for index, partition in enumerate(media_partitions):
            # duplicate posts for each partition
            if len(media_partitions) > 1:
                _post_text = f"[{index + 1}/{len(media_partitions)}] {post.title}"
            else:
                _post_text = post.title

            # truncate text if it exceeds the limit
            if len(_post_text) > self.config.post_text_limit:
                _post_text = (
                    _post_text[
                        : self.config.post_text_limit
                        - len(self.config.post_text_truncate_suffix)
                    ]
                    + self.config.post_text_truncate_suffix
                )

            text_builder = parse_to_richtext(_post_text)
            logger.debug(f"Post text: {text_builder.build_text()}")
            logger.debug(f"Post facets: {text_builder.build_facets()}")

            # all paths to embeds now must be relative to current working directory
            migration_job = BlueSkyMigrationJob(
                job_index=len(self.migration_queue.queue),
                archive_index=archive_index,
                text=_post_text,
                rich_text=text_builder.build_text(),
                facets=text_builder.build_facets(),
                created_at=(
                    # convert timestamp into expected format
                    datetime.fromtimestamp(post.creation_timestamp).isoformat()
                    + "Z"
                ),
                embed=partition,
            )

            if index == 0:
                migration_job.root_index = None
                migration_job.parent_index = None
                thread_start = migration_job.job_index
                reply_to = migration_job.job_index
            else:
                migration_job.root_index = thread_start
                migration_job.parent_index = reply_to
                reply_to = migration_job.job_index

            # add the job to the queue
            self.migration_queue.append(migration_job)


class BlueSkyPostingEngine:
    """Engine to post to Bluesky."""

    def __init__(
        self,
        archive_folder: str,
        config: MigrationConfig,
        username: str = None,
        password: str = None,
        simulate: bool = True,
    ):
        """Initialize the engine.

        Args:
            config: MigrationConfig object.
            username: Bluesky username.
            password: Bluesky password.
            simulate: Whether to migrate posts or not.
        """
        self.archive_folder = archive_folder
        self.config = config
        self.username = username
        self.password = password
        self.simulate = simulate

        # initialize the client
        self.client = Client(config.endpoint)

        self.queue = MigrationQueue(self.config.queue_dir)
        self.queue.load()

    def _post_to_bluesky(self, migration_job: BlueSkyMigrationJob):
        """Post a post from our queue into Bluesky."""
        logger.info(
            f"Posting job job_index={migration_job.job_index} archive_index={migration_job.archive_index} to Bluesky."
        )
        if self.simulate:
            logger.debug(f"Simulating post: {migration_job.text}")
            return

        if migration_job.state == BlueSkyMigrationJobState.PROCESSED:
            logger.debug(
                f"Skipping job job_index={migration_job.job_index} archive_index={migration_job.archive_index} - state: {migration_job.state}"
            )
            return

        # identify partition type
        if len(migration_job.embed) >= 1:
            partition_type = (
                "video" if migration_job.embed[0].uri.endswith(".mp4") else "image"
            )
        else:
            raise ValueError("Partition has 0 media which is not possible.")

        # upload the media
        if partition_type == "video":
            logger.info(f"Uploading video: {migration_job.embed[0].uri}")
            with open(
                Path(self.archive_folder) / migration_job.embed[0].uri, "rb"
            ) as f:
                img_data = f.read()

            blob = self.client.upload_blob(img_data)
            embeds = models.AppBskyEmbedVideo.Main(video=blob.blob)
        elif partition_type == "image":
            embeds = []
            for embed in migration_job.embed:
                logger.info(f"Uploading image: {embed.uri}")

                with open(Path(self.archive_folder) / embed.uri, "rb") as f:
                    img_data = f.read()

                blob = self.client.upload_blob(img_data)
                embeds.append(
                    models.AppBskyEmbedImages.Image(alt="Img alt", image=blob.blob)
                )

            embeds = models.AppBskyEmbedImages.Main(images=embeds)

        # post the media
        rich_text_builder = parse_to_richtext(migration_job.text)
        _record_args = dict(
            text=rich_text_builder.build_text(),
            facets=rich_text_builder.build_facets(),
            embed=embeds,
            created_at=migration_job.created_at,
        )
        if (
            migration_job.root_index is not None
            and migration_job.parent_index is not None
        ):
            logger.info(
                f"Posting reply to root_index={migration_job.root_index} parent_index={migration_job.parent_index}"
            )
            root_job = self.queue.queue[migration_job.root_index]
            logger.debug(f"root cid: {root_job.cid}")
            logger.debug(f"root uri: {root_job.uri}")
            root_ref = models.create_strong_ref(
                root_job
            )  # jobs have uri and cid fields

            parent_job = self.queue.queue[migration_job.parent_index]
            logger.debug(f"parent cid: {parent_job.cid}")
            logger.debug(f"parent uri: {parent_job.uri}")
            parent_ref = models.create_strong_ref(parent_job)

            _record_args["reply"] = models.AppBskyFeedPost.ReplyRef(
                parent=parent_ref, root=root_ref
            )

        # response = self.client.post(**_record_args)
        post = models.AppBskyFeedPost.Record(**_record_args)
        response = self.client.app.bsky.feed.post.create(self.client.me.did, post)
        logger.debug(f"Posted to Bluesky: {response.uri}")

        migration_job.cid = response.cid
        migration_job.uri = response.uri
        migration_job.state = BlueSkyMigrationJobState.PROCESSED
        self.queue.save(migration_job)

    def post(self, index: Optional[int] = None):
        """Post a post from our queue into Bluesky."""
        # login to Bluesky
        if not self.simulate:
            logger.info("Logging in to Bluesky...")
            self.client.login(self.username, self.password)

        if isinstance(index, int):
            for post in self.queue.queue:
                if post.archive_index == index:
                    logger.info(
                        f"Posting job archive_index={index} job_index={post.job_index} only."
                    )
                    self._post_to_bluesky(post)
            return

        for index, post in enumerate(self.queue.queue):
            if post.state == BlueSkyMigrationJobState.PROCESSED:
                logger.debug(
                    f"Skipping post job_index={post.job_index} archive_index={post.archive_index} - state: {post.state}"
                )
                continue

            if index > 0:
                time.sleep(self.config.api_rate_limit_delay_secs)

            if (
                post.state == BlueSkyMigrationJobState.READY
                or post.state == BlueSkyMigrationJobState.FAILED
            ):
                logger.info(
                    f"Posting to Bluesky [{index}/{len(self.queue.queue)}]: {post.text}"
                )
                self._post_to_bluesky(post)

    def rollback(self):
        """Rollback posts from our queue into Bluesky."""
        # login to Bluesky
        if not self.simulate:
            logger.info("Logging in to Bluesky...")
            self.client.login(self.username, self.password)

        for index, post in enumerate(self.queue.queue):
            if post.state == BlueSkyMigrationJobState.READY:
                logger.debug(
                    f"Skipping post job_index={post.job_index} archive_index={post.archive_index} - state: {post.state}"
                )
                continue

            if (
                post.state == BlueSkyMigrationJobState.PROCESSED
                or post.state == BlueSkyMigrationJobState.FAILED
            ):
                logger.info(
                    f"Rolling back Bluesky [{index}/{len(self.queue.queue)}]: {post.text}"
                )
                # delete the post using cid/uri
                self.client.delete_post(post.uri)
                # update the state
                post.state = BlueSkyMigrationJobState.READY
                self.queue.save(post)
