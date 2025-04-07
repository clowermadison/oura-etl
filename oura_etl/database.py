"""Database module for the Oura ETL pipeline.

This module handles database connections and defines SQLAlchemy models
for storing Oura Ring data.
"""

from sqlalchemy import create_engine, Column, Integer, Float, String, Boolean, ForeignKey, JSON, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.types import TypeDecorator, VARCHAR
from typing import Optional, Dict, Any
import json
import enum

Base = declarative_base()

# Define all enums first
class DailyStressSummary(enum.Enum):
    """Daily stress summary types."""
    restored = "restored"
    normal = "normal"
    stressful = "stressful"

class SleepAlgorithmVersion(enum.Enum):
    """Sleep algorithm versions."""
    v1 = "v1"
    v2 = "v2"

class HeartRateSource(enum.Enum):
    """Heart rate source types."""
    awake = "awake"
    rest = "rest"
    sleep = "sleep"
    session = "session"
    live = "live"
    workout = "workout"

class MomentMood(enum.Enum):
    """Moment mood types."""
    bad = "bad"
    worse = "worse"
    same = "same"
    good = "good"
    great = "great"

class MomentType(enum.Enum):
    """Moment type values."""
    breathing = "breathing"
    meditation = "meditation"
    nap = "nap"
    relaxation = "relaxation"
    rest = "rest"
    body_status = "body_status"

class LongTermResilienceLevel(enum.Enum):
    """Long term resilience levels."""
    limited = "limited"
    adequate = "adequate"
    solid = "solid"
    strong = "strong"
    exceptional = "exceptional"

class RingColor(enum.Enum):
    """Ring color options."""
    brushed_silver = "brushed_silver"
    glossy_black = "glossy_black"
    glossy_gold = "glossy_gold"
    glossy_white = "glossy_white"
    gucci = "gucci"
    matt_gold = "matt_gold"
    rose = "rose"
    silver = "silver"
    stealth_black = "stealth_black"
    titanium = "titanium"
    titanium_and_gold = "titanium_and_gold"

class RingDesign(enum.Enum):
    """Ring design options."""
    balance = "balance"
    balance_diamond = "balance_diamond"
    heritage = "heritage"
    horizon = "horizon"

class RingHardwareType(enum.Enum):
    """Ring hardware type options."""
    gen1 = "gen1"
    gen2 = "gen2"
    gen2m = "gen2m"
    gen3 = "gen3"
    gen4 = "gen4"

class WorkoutIntensity(enum.Enum):
    """Workout intensity levels."""
    easy = "easy"
    moderate = "moderate"
    hard = "hard"

class JSONEncodedDict(TypeDecorator):
    """Represents an immutable structure as a json-encoded string."""
    impl = VARCHAR

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = json.dumps(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = json.loads(value)
        return value

class ActivityContributors(Base):
    """Activity contributors database model."""
    __tablename__ = 'activity_contributors'

    id = Column(String, primary_key=True)
    daily_activity_id = Column(String, ForeignKey('daily_activity.id'))
    meet_daily_targets = Column(Integer)
    move_every_hour = Column(Integer)
    recovery_time = Column(Integer)
    stay_active = Column(Integer)
    training_frequency = Column(Integer)
    training_volume = Column(Integer)

    daily_activity = relationship("DailyActivity", back_populates="contributors")

class ActivityMetrics(Base):
    """Activity metrics database model."""
    __tablename__ = 'activity_metrics'

    id = Column(String, primary_key=True)
    daily_activity_id = Column(String, ForeignKey('daily_activity.id'))
    timestamp = Column(String)
    interval = Column(Float)
    value = Column(Float)

    daily_activity = relationship("DailyActivity", back_populates="met_values")

class DailyActivity(Base):
    """Daily activity database model."""
    __tablename__ = 'daily_activity'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    class_5_min = Column(String, nullable=True)
    score = Column(Integer, nullable=True)
    active_calories = Column(Integer)
    average_met_minutes = Column(Float)
    equivalent_walking_distance = Column(Integer)
    high_activity_met_minutes = Column(Integer)
    high_activity_time = Column(Integer)
    inactivity_alerts = Column(Integer)
    low_activity_met_minutes = Column(Integer)
    low_activity_time = Column(Integer)
    medium_activity_met_minutes = Column(Integer)
    medium_activity_time = Column(Integer)
    met = Column(JSONEncodedDict, nullable=True)
    meters_to_target = Column(Integer)
    non_wear_time = Column(Integer)
    resting_time = Column(Integer)
    sedentary_met_minutes = Column(Integer)
    sedentary_time = Column(Integer)
    steps = Column(Integer)
    target_calories = Column(Integer)
    target_meters = Column(Integer)
    total_calories = Column(Integer)
    timestamp = Column(String)

    contributors = relationship("ActivityContributors", back_populates="daily_activity", uselist=False)
    met_values = relationship("ActivityMetrics", back_populates="daily_activity")

class SleepContributors(Base):
    """Sleep contributors database model."""
    __tablename__ = 'sleep_contributors'

    id = Column(String, primary_key=True)
    daily_sleep_id = Column(String, ForeignKey('daily_sleep.id'))
    deep_sleep = Column(Integer)
    efficiency = Column(Integer)
    latency = Column(Integer)
    rem_sleep = Column(Integer)
    restfulness = Column(Integer)
    timing = Column(Integer)
    total_sleep = Column(Integer)

    daily_sleep = relationship("DailySleep", back_populates="contributors")

class HeartRateSample(Base):
    """Heart rate sample database model."""
    __tablename__ = 'heart_rate_samples'

    id = Column(String, primary_key=True)
    sleep_id = Column(String, ForeignKey('sleep.id'))
    timestamp = Column(String)
    interval = Column(Float)
    value = Column(Float)

    sleep = relationship("Sleep", back_populates="heart_rate_samples")

class HRVSample(Base):
    """HRV sample database model."""
    __tablename__ = 'hrv_samples'

    id = Column(String, primary_key=True)
    sleep_id = Column(String, ForeignKey('sleep.id'))
    timestamp = Column(String)
    interval = Column(Float)
    value = Column(Float)

    sleep = relationship("Sleep", back_populates="hrv_samples")

class Sleep(Base):
    """Detailed sleep period data."""
    __tablename__ = 'sleep'

    id = Column(String, primary_key=True)
    average_breath = Column(Float, nullable=True)
    average_heart_rate = Column(Float, nullable=True)
    average_hrv = Column(Integer, nullable=True)
    awake_time = Column(Integer, nullable=True)
    bedtime_end = Column(String)
    bedtime_start = Column(String)
    day = Column(String, index=True)
    deep_sleep_duration = Column(Integer, nullable=True)
    efficiency = Column(Integer, nullable=True)
    heart_rate = Column(JSONEncodedDict, nullable=True)
    hrv = Column(JSONEncodedDict, nullable=True)
    latency = Column(Integer, nullable=True)
    light_sleep_duration = Column(Integer, nullable=True)
    low_battery_alert = Column(Boolean)
    lowest_heart_rate = Column(Integer, nullable=True)
    movement_30_sec = Column(String, nullable=True)
    period = Column(Integer)
    readiness = Column(JSONEncodedDict, nullable=True)
    readiness_score_delta = Column(Integer, nullable=True)
    rem_sleep_duration = Column(Integer, nullable=True)
    restless_periods = Column(Integer, nullable=True)
    sleep_phase_5_min = Column(String, nullable=True)
    sleep_score_delta = Column(Integer, nullable=True)
    sleep_algorithm_version = Column(Enum(SleepAlgorithmVersion), nullable=True)
    time_in_bed = Column(Integer)
    total_sleep_duration = Column(Integer, nullable=True)
    type = Column(String)

    heart_rate_samples = relationship("HeartRateSample", back_populates="sleep")
    hrv_samples = relationship("HRVSample", back_populates="sleep")

class DailySleep(Base):
    """Daily sleep database model."""
    __tablename__ = 'daily_sleep'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    score = Column(Integer, nullable=True)
    timestamp = Column(String)

    contributors = relationship("SleepContributors", back_populates="daily_sleep", uselist=False)

class ReadinessContributors(Base):
    """Readiness contributors database model."""
    __tablename__ = 'readiness_contributors'

    id = Column(String, primary_key=True)
    daily_readiness_id = Column(String, ForeignKey('daily_readiness.id'))
    activity_balance = Column(Integer)
    body_temperature = Column(Integer)
    hrv_balance = Column(Integer)
    previous_day_activity = Column(Integer)
    previous_night = Column(Integer)
    recovery_index = Column(Integer)
    resting_heart_rate = Column(Integer)
    sleep_balance = Column(Integer)

    daily_readiness = relationship("DailyReadiness", back_populates="contributors")

class DailyReadiness(Base):
    """Daily readiness database model."""
    __tablename__ = 'daily_readiness'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    score = Column(Integer, nullable=True)
    temperature_deviation = Column(Float, nullable=True)
    temperature_trend_deviation = Column(Float, nullable=True)
    timestamp = Column(String)

    contributors = relationship("ReadinessContributors", back_populates="daily_readiness", uselist=False)

class SPO2Percentage(Base):
    """SPO2 percentage database model."""
    __tablename__ = 'spo2_percentage'

    id = Column(String, primary_key=True)
    daily_spo2_id = Column(String, ForeignKey('daily_spo2.id'))
    average = Column(Float)
    timestamp = Column(String)

    daily_spo2 = relationship("DailySPO2", back_populates="spo2_percentage")

class DailySPO2(Base):
    """Daily SPO2 database model."""
    __tablename__ = 'daily_spo2'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    breathing_disturbance_index = Column(Integer)
    timestamp = Column(String)

    spo2_percentage = relationship("SPO2Percentage", back_populates="daily_spo2", uselist=False)

class ResilienceContributors(Base):
    """Resilience contributors database model."""
    __tablename__ = 'resilience_contributors'

    id = Column(String, primary_key=True)
    daily_resilience_id = Column(String, ForeignKey('daily_resilience.id'))
    sleep_recovery = Column(Float)
    daytime_recovery = Column(Float)
    stress = Column(Float)

    daily_resilience = relationship("DailyResilience", back_populates="contributors")

class DailyResilience(Base):
    """Daily resilience database model."""
    __tablename__ = 'daily_resilience'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    level = Column(Enum(LongTermResilienceLevel), nullable=True)
    timestamp = Column(String)

    contributors = relationship("ResilienceContributors", back_populates="daily_resilience", uselist=False)

class HeartRate(Base):
    """Heart rate database model."""
    __tablename__ = 'heart_rate'

    id = Column(String, primary_key=True)
    bpm = Column(Integer)
    source = Column(Enum(HeartRateSource))
    timestamp = Column(String, index=True)

class PersonalInfo(Base):
    """Personal info database model."""
    __tablename__ = 'personal_info'

    id = Column(String, primary_key=True)
    age = Column(Integer)
    weight = Column(Float)
    height = Column(Float)
    biological_sex = Column(String)
    email = Column(String)

class SleepTime(Base):
    """Sleep time database model."""
    __tablename__ = 'sleep_time'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    bedtime_start = Column(String)
    bedtime_end = Column(String)
    timezone = Column(Integer)
    timestamp = Column(String)

class DailyHRV(Base):
    """Daily HRV database model."""
    __tablename__ = 'daily_hrv'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    night_timestamp = Column(String)
    rmssd = Column(Float)
    rmssd_samples = Column(Integer)
    sample_duration = Column(Float)
    score = Column(Integer)
    timestamp = Column(String)

class DailyStress(Base):
    """Daily stress database model."""
    __tablename__ = 'daily_stress'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    stress_high = Column(Integer)
    recovery_high = Column(Integer)
    day_summary = Column(Enum(DailyStressSummary))
    timestamp = Column(String)

class RingConfiguration(Base):
    """Ring configuration database model."""
    __tablename__ = 'ring_configuration'

    id = Column(String, primary_key=True)
    color = Column(Enum(RingColor))
    design = Column(Enum(RingDesign))
    firmware_version = Column(String)
    hardware_type = Column(Enum(RingHardwareType))
    set_up_at = Column(String)
    size = Column(Integer)

class DailyCardiovascularAge(Base):
    """Daily cardiovascular age database model."""
    __tablename__ = 'daily_cardiovascular_age'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    age = Column(Integer)
    timestamp = Column(String)

class VO2Max(Base):
    """VO2 Max database model."""
    __tablename__ = 'vo2_max'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    value = Column(Float)
    timestamp = Column(String)

class Workout(Base):
    """Workout database model."""
    __tablename__ = 'workout'

    id = Column(String, primary_key=True)
    activity = Column(String)
    calories = Column(Integer)
    day = Column(String, index=True)
    distance = Column(Float)
    intensity = Column(Enum(WorkoutIntensity))
    label = Column(String)
    source = Column(String)
    start_datetime = Column(String)
    end_datetime = Column(String)
    timestamp = Column(String)

class Session(Base):
    """Session database model."""
    __tablename__ = 'session'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    start_datetime = Column(String)
    end_datetime = Column(String)
    type = Column(String)
    mood = Column(String, nullable=True)
    timestamp = Column(String)

class Tag(Base):
    """Tag database model."""
    __tablename__ = 'tag'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    text = Column(String)
    timestamp = Column(String)

class EnhancedTag(Base):
    """Enhanced tag database model."""
    __tablename__ = 'enhanced_tag'

    id = Column(String, primary_key=True)
    day = Column(String, index=True)
    tag_type = Column(String)
    text = Column(String)
    timestamp = Column(String)

class RestModePeriod(Base):
    """Rest mode period database model."""
    __tablename__ = 'rest_mode_period'

    id = Column(String, primary_key=True)
    start_time = Column(String)
    end_time = Column(String)
    state = Column(String)
    timestamp = Column(String)

class Database:
    """Database connection and session management."""
    
    def __init__(self, url: Optional[str] = None):
        """Initialize database connection.
        
        Args:
            url: Optional database connection URL. If not provided, uses config.toml setting.
        """
        if url is None:
            from oura_etl.utils.load_config import get_db_config
            db_config = get_db_config()
            url = f"sqlite:///{db_config.db_path}"
            
        self.engine = create_engine(url)
        self.Session = sessionmaker(bind=self.engine)
        
    def create_tables(self):
        """Create all database tables."""
        Base.metadata.create_all(self.engine)
        
    def get_session(self):
        """Get a new database session."""
        return self.Session()
        
    def save_records(self, records: list):
        """Save a list of records to the database.
        
        Args:
            records: List of SQLAlchemy model instances
        """
        session = self.get_session()
        try:
            for record in records:
                session.merge(record)  # Use merge instead of add to handle duplicates
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

# Map data types to database models
MODEL_MAP = {
    'daily_activity': DailyActivity,
    'daily_readiness': DailyReadiness,
    'daily_sleep': DailySleep,
    # TODO: Uncomment these when ready to load
    # 'sleep': Sleep,
    # 'heart_rate': HeartRate,
    # 'personal_info': PersonalInfo,
    # 'sleep_time': SleepTime,
    # 'daily_hrv': DailyHRV,
    # 'daily_spo2': DailySPO2,
    # 'daily_stress': DailyStress,
    # 'daily_resilience': DailyResilience,
    # 'daily_cardiovascular_age': DailyCardiovascularAge,
    # 'vo2_max': VO2Max,
    # 'workout': Workout,
    # 'session': Session,
    # 'tag': Tag,
    # 'enhanced_tag': EnhancedTag,
    # 'rest_mode_period': RestModePeriod,
    # 'ring_configuration': RingConfiguration,
    # 'activity_contributors': ActivityContributors,
    # 'activity_metrics': ActivityMetrics,
    # 'sleep_contributors': SleepContributors,
    # 'heart_rate_samples': HeartRateSample,
    # 'hrv_samples': HRVSample,
    # 'readiness_contributors': ReadinessContributors,
    # 'spo2_percentage': SPO2Percentage,
    # 'resilience_contributors': ResilienceContributors
} 