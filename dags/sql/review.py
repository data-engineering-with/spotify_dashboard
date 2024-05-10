def select_review(bucket_name: str, date: str=None) :
    sql = f"""
    USE SCHEMA PUBLIC;

    CREATE TABLE IF NOT EXISTS reviews(
        id bigint NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
        spotify_track_id varchar NULL,
        reviews_date datetime NULL,
        contents varchar NULL,
        likes bigint NULL
    );


    CREATE OR REPLACE FILE FORMAT my_csv_format
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1;


    CREATE OR REPLACE STAGE reviews_stage
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{bucket_name}/transform/last_fm/reviews/'
        FILE_FORMAT = my_csv_format;
        
        
    COPY INTO SPOTIFY.PUBLIC.REVIEWS (spotify_track_id, contents, reviews_date, likes) 
    FROM (
        SELECT $1spotify_track_id, $2contents, $3reviews_date, $4likes
        FROM '@reviews_stage'
    )
    FILE_FORMAT = my_csv_format
    ON_ERROR = 'ABORT_STATEMENT';  
    
    COMMIT
    """
    
    return sql