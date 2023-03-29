CREATE STREAM users_stream (id INT, firstName VARCHAR, lastName VARCHAR)
    WITH (kafka_topic='users', value_format='JSON');

CREATE STREAM users_name AS 
    SELECT id, firstName, lastName 
    FROM users_stream
    EMIT CHANGES;


CREATE STREAM users_age AS 
    SELECT * 
    FROM users_stream 
    WHERE gender = 'female'
    EMIT CHANGES;

CREATE MATERIALIZED VIEW people_with_generation AS
SELECT id, 
  CASE 
    WHEN age < 25 THEN 'Gen Z'
    WHEN age BETWEEN 25 AND 35 THEN 'Millennials'
    WHEN age BETWEEN 36 AND 45 THEN 'Gen X'
    WHEN age > 45 THEN 'Boomers'
    ELSE NULL
  END AS generation
FROM people;