"""ORM models for the Devalon marketing agent (Turso / sqlite).

Table 1 — week_trendings: one row per weekly trend run, holds the R2
source URLs (themes / trends / dropped CSVs).

Table 2 — posts: the generated channel copy, FK -> week_trendings.
``CHANNELS`` keys in smm_agent.py map as:
  ig_company -> (instagram, company)    fb_company -> (facebook, company)
  li_company -> (linkedin, company)     li_personal -> (linkedin, personal)
"""
from sqlalchemy import CheckConstraint, ForeignKey, Index, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, validates


class Base(DeclarativeBase):
    pass


PLATFORMS = ("facebook", "instagram", "linkedin")
ACCOUNT_TYPES = ("company", "personal")


class WeekTrending(Base):
    __tablename__ = "week_trendings"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    week_label: Mapped[str] = mapped_column(String(16), unique=True, nullable=False, index=True)
    themes_url: Mapped[str] = mapped_column(Text, nullable=False)
    trends_url: Mapped[str] = mapped_column(Text, nullable=False)
    dropped_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)
    updated_at: Mapped[str] = mapped_column(server_default=func.now(), onupdate=func.now(), nullable=False)

    posts: Mapped[list["Post"]] = relationship(
        back_populates="week", cascade="all, delete-orphan", passive_deletes=True
    )


class Post(Base):
    __tablename__ = "posts"
    __table_args__ = (
        UniqueConstraint("week_id", "platform", "account_type", "version",
                         name="uq_post_week_channel_version"),
        CheckConstraint(f"platform IN {PLATFORMS}", name="ck_post_platform"),
        CheckConstraint(f"account_type IN {ACCOUNT_TYPES}", name="ck_post_account"),
        Index("ix_posts_week_id", "week_id"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    week_id: Mapped[int] = mapped_column(
        ForeignKey("week_trendings.id", ondelete="CASCADE"), nullable=False
    )
    platform: Mapped[str] = mapped_column(String(16), nullable=False)
    account_type: Mapped[str] = mapped_column(String(16), nullable=False)
    headline: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    # Array of bare tag strings (no '#'), 10-20 items. Stored as JSON text on sqlite/libsql.
    hashtags: Mapped[list] = mapped_column(JSON, nullable=False)
    model: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # Generation counter per week: reruns of the same week get 2, 3, ...
    version: Mapped[int] = mapped_column(default=1, server_default="1", nullable=False)
    created_at: Mapped[str] = mapped_column(server_default=func.now(), nullable=False)
    updated_at: Mapped[str] = mapped_column(server_default=func.now(), onupdate=func.now(), nullable=False)

    week: Mapped[WeekTrending] = relationship(back_populates="posts")

    @validates("platform")
    def _validate_platform(self, _key, value):
        if value not in PLATFORMS:
            raise ValueError(f"platform must be one of {PLATFORMS}, got {value!r}")
        return value

    @validates("account_type")
    def _validate_account(self, _key, value):
        if value not in ACCOUNT_TYPES:
            raise ValueError(f"account_type must be one of {ACCOUNT_TYPES}, got {value!r}")
        return value

    @validates("hashtags")
    def _validate_hashtags(self, _key, value):
        if not isinstance(value, list) or not all(isinstance(t, str) for t in value):
            raise ValueError("hashtags must be a list of strings")
        if not (10 <= len(value) <= 20):
            raise ValueError(f"hashtags need 10-20 items, got {len(value)}")
        return value
