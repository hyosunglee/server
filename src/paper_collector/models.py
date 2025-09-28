import enum
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    Boolean,
    DateTime,
    ForeignKey,
    Enum,
    UniqueConstraint,
    Float
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

Base = declarative_base()

class Work(Base):
    __tablename__ = 'works'
    id = Column(Integer, primary_key=True)
    doi = Column(String, unique=True, nullable=False, index=True)
    title = Column(String)
    year = Column(Integer, index=True)
    venue = Column(String)
    abstract = Column(Text)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    fingerprint = Column(String) # hash(title+authors+year)

    # Relationships
    identifier = relationship("Identifier", back_populates="work", uselist=False)
    access = relationship("Access", back_populates="work", uselist=False)
    summaries = relationship("Summary", back_populates="work")

class Identifier(Base):
    __tablename__ = 'identifiers'
    id = Column(Integer, primary_key=True)
    work_id = Column(Integer, ForeignKey('works.id'), nullable=False)
    doi = Column(String, unique=True, nullable=False, index=True)
    openalex_id = Column(String, unique=True)
    pmid = Column(String)
    pmcid = Column(String)
    arxiv_id = Column(String)

    # Relationship
    work = relationship("Work", back_populates="identifier")

class Access(Base):
    __tablename__ = 'access'
    id = Column(Integer, primary_key=True)
    work_id = Column(Integer, ForeignKey('works.id'), nullable=False)
    doi = Column(String, unique=True, nullable=False, index=True)
    is_oa = Column(Boolean, index=True)
    oa_status = Column(String)
    license = Column(String)
    oa_urls = Column(JSONB)
    checked_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    work = relationship("Work", back_populates="access")

class SummaryBasedOn(enum.Enum):
    pdf = "pdf"
    abstract = "abstract"

class Summary(Base):
    __tablename__ = 'summaries'
    id = Column(Integer, primary_key=True)
    work_id = Column(Integer, ForeignKey('works.id'), nullable=False)
    doi = Column(String, nullable=False, index=True)
    model_version = Column(String)
    based_on = Column(Enum(SummaryBasedOn), nullable=False)
    summary_short = Column(Text)
    key_claims = Column(JSONB)
    confidence = Column(Float)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationship
    work = relationship("Work", back_populates="summaries")
    __table_args__ = (UniqueConstraint('doi', 'model_version', 'based_on', name='_doi_model_based_on_uc'),)


class Job(Base):
    __tablename__ = 'jobs'
    id = Column(Integer, primary_key=True)
    type = Column(String)
    status = Column(String)
    payload = Column(JSONB)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True))
    error = Column(Text)