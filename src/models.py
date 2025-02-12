from typing import Any, Dict, List, Optional
import pydantic
from enum import Enum
from datetime import datetime


class MigrationMediaStrategy(str, Enum):
    ORDERED = "ordered"
    VIDEOLAST = "videolast"


class MigrationConfig(pydantic.BaseModel, extra="allow"):
    # migration engine params
    min_date: Optional[datetime] = None
    max_date: Optional[datetime] = None
    queue_dir: str
    endpoint: str
    media_strategy: Optional[MigrationMediaStrategy] = MigrationMediaStrategy.ORDERED

    # service limits
    max_images_per_post: int = 4
    post_text_limit: int = 300
    post_text_truncate_suffix: str = "..."
    api_rate_limit_delay_secs: int = 3


###############################################################
# A couple of classes to help with the Instagram archive JSON #
###############################################################


class InstagramArchiveMediaMetadata(pydantic.BaseModel, extra="allow"):
    camera_metadata: Optional[Dict[str, Any]] = {}


class InstagramArchiveMedia(pydantic.BaseModel, extra="allow"):
    uri: str
    media_metadata: Dict
    creation_timestamp: Optional[int] = None
    cross_post_source: Optional[Dict[str, str]] = {}
    backup_uri: Optional[str] = None


class InstagramArchivePost(pydantic.BaseModel, extra="allow"):
    media: List[InstagramArchiveMedia]
    title: Optional[str] = ""
    creation_timestamp: Optional[int] = None


class InstagramArchive(pydantic.BaseModel, extra="allow"):
    posts: List[InstagramArchivePost]


class BlueSkyMigrationJobState(str, Enum):
    # the job is ready to be processed
    READY = "ready"

    # the job has been processed
    PROCESSED = "processed"

    # the job has failed
    FAILED = "failed"


class BlueSkyMigrationJob(pydantic.BaseModel, extra="allow"):
    # the index of the post in the Instagram archive
    archive_index: int

    # the test of the post
    text: str
    rich_text: str
    facets: List[Any] = []

    # the date of creation (original)
    created_at: str  # ISO format

    # list of embeds (images, videos)
    embed: List[InstagramArchiveMedia] = []

    # a list of other migration jobs
    thread: Optional[List[Any]] = []

    state: BlueSkyMigrationJobState = BlueSkyMigrationJobState.READY

    job_index: int
    root_index: Optional[int] = None
    parent_index: Optional[int] = None

    cid: Optional[str] = None
    uri: Optional[str] = None
