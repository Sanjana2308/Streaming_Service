CREATE DATABASE Streaming_temp2;

USE Streaming_temp2


-- Create table for storing user profiles
CREATE TABLE user_dim (
    user_id INT PRIMARY KEY,
    user_name VARCHAR(255),
    location VARCHAR(255),
    age_group VARCHAR(50)
);


-- Create table for storing content metadata
CREATE TABLE content_dim (
    content_id INT PRIMARY KEY,
    title VARCHAR(255),
    genre VARCHAR(255),
    release_year INT
);


-- Create table for subscription_plans
CREATE TABLE subscription_plan_dim (
    plan_id INT PRIMARY KEY,
    plan_name VARCHAR(255),
    price DECIMAL(5, 2),
    features TEXT 
);


-- Create table for devices used
CREATE TABLE device_dim (
    device_id INT PRIMARY KEY,
    device_type VARCHAR(255), 
    operating_system VARCHAR(255), 
    manufacturer VARCHAR(255) 
);



-- Create a single fact table for all user interactions, subscriptions, devices, and activities
CREATE TABLE unified_interaction_fact (
    interaction_id INT PRIMARY KEY,
    user_id INT, -- Foreign key from user_dim
    content_id INT, -- Foreign key from content_dim
    plan_id INT, -- Foreign key from subscription_plan_dim
    device_id INT, -- Foreign key from device_dim
    watch_time INT, -- Watch time in minutes
    rating DECIMAL(2, 1), -- Rating given by the user on a scale of 1 to 5
    activity_type VARCHAR(255), -- Activity type like 'like', 'share', 'comment'
    activity_timestamp DATETIME, -- DATETIME for the user activity
    interaction_date DATETIME -- DATETIME for the interaction (watching content)
);



-- INSERT Statements For user_dim
INSERT INTO user_dim (user_id, user_name, location, age_group) VALUES
(1, 'Alice Smith', 'New York', '18-24'),
(2, 'Bob Johnson', 'Los Angeles', '25-34'),
(3, 'Charlie Brown', 'Chicago', '35-44'),
(4, 'Diana Prince', 'Houston', '18-24'),
(5, 'Ethan Hunt', 'Phoenix', '25-34'),
(6, 'Fiona Glenanne', 'Philadelphia', '35-44'),
(7, 'George Costanza', 'San Francisco', '45-54');


-- INSERT Statements For content_dim
INSERT INTO content_dim (content_id, title, genre, release_year) VALUES
(101, 'Inception', 'Sci-Fi', 2010),
(102, 'The Office', 'Comedy', 2005),
(103, 'Breaking Bad', 'Drama', 2008),
(104, 'Stranger Things', 'Horror', 2016),
(105, 'The Crown', 'Biography', 2016),
(106, 'Game of Thrones', 'Fantasy', 2011),
(107, 'Avengers: Endgame', 'Action', 2019);


-- INSERT Statements For subscription_plan_dim
INSERT INTO subscription_plan_dim (plan_id, plan_name, price, features) VALUES
(1, 'Basic', 9.99, 'HD, No Ads'),
(2, 'Standard', 14.99, 'HD, 2 Screens, No Ads'),
(3, 'Premium', 19.99, '4K, 4 Screens, No Ads'),
(4, 'Family', 24.99, 'HD, 5 Screens, No Ads, Family Sharing'),
(5, 'Student', 4.99, 'HD, No Ads, Limited Content'),
(6, 'Annual', 99.99, 'HD, No Ads, Annual Discount'),
(7, 'Trial', 0.00, 'HD, 30 Days Free');


-- INSERT Statements for device_dim
INSERT INTO device_dim (device_id, device_type, operating_system, manufacturer) VALUES
(201, 'Mobile', 'iOS', 'Apple'),
(202, 'Tablet', 'Android', 'Samsung'),
(203, 'Smart TV', 'Tizen', 'Samsung'),
(204, 'Desktop', 'Windows', 'Dell'),
(205, 'Laptop', 'macOS', 'Apple'),
(206, 'Smartphone', 'Android', 'Google'),
(207, 'Smart TV', 'Android', 'Sony');


-- INSERT Statements for unified_interaction_fact
INSERT INTO unified_interaction_fact (interaction_id, user_id, content_id, plan_id, device_id, watch_time, rating, activity_type, activity_timestamp, interaction_date) VALUES
(1001, 1, 101, 3, 201, 120, 4.5, 'watch', '2024-09-15 12:00:00', '2024-09-15 12:34:56'),
(1002, 2, 102, 1, 202, 180, 5.0, 'like', '2024-09-16 14:00:00', '2024-09-16 14:22:31'),
(1003, 3, 103, 2, 203,  150, 4.8, 'share', '2024-09-17 16:00:00', '2024-09-17 16:45:22'),
(1004, 4, 104, 4, 204, 95, 4.2, 'comment', '2024-09-18 11:00:00', '2024-09-18 11:20:15'),
(1005, 5, 105, 5, 205, 200, 4.9, 'watch', '2024-09-19 09:00:00', '2024-09-19 09:10:45'),
(1006, 6, 106, 3, 206, 30, 3.9, 'like', '2024-09-20 10:00:00', '2024-09-20 10:45:00'),
(1007, 7, 107, 2, 207, 210, 4.6, 'share', '2024-09-21 13:00:00', '2024-09-21 13:15:00')


SELECT * FROM content_dim;
SELECT * FROM device_dim;
SELECT * FROM subscription_plan_dim;
SELECT * FROM user_dim;
SELECT * FROM unified_interaction_fact;


-- Retrieve Top 5 Trending Content by Watch Time
SELECT TOP 5 c.title, SUM(uif.watch_time) AS total_watch_time
FROM unified_interaction_fact uif
JOIN content_dim c ON uif.content_id = c.content_id
GROUP BY c.title
ORDER BY total_watch_time DESC;


-- Find User Preferences (Most Watched Genres)
SELECT c.genre, SUM(uif.watch_time) AS total_watch_time
FROM unified_interaction_fact uif
JOIN content_dim c ON uif.content_id = c.content_id
GROUP BY c.genre
ORDER BY total_watch_time DESC;


-- Retrieve Top 5 Rated Content
SELECT TOP 5 c.title, AVG(uif.rating) AS average_rating
FROM unified_interaction_fact uif
JOIN content_dim c ON uif.content_id = c.content_id
GROUP BY c.title
ORDER BY average_rating DESC;


-- Retrieve Top 5 Trending Content by Watch Time for Premium Users
SELECT TOP 5 c.title, SUM(uif.watch_time) AS total_watch_time
FROM unified_interaction_fact uif
JOIN content_dim c ON uif.content_id = c.content_id
WHERE uif.plan_id = 1 
GROUP BY c.title
ORDER BY total_watch_time DESC;


-- Get Most Used Devices by Users in a Specific Age Group
SELECT d.device_type, COUNT(uif.device_id) AS usage_count
FROM unified_interaction_fact uif
JOIN user_dim u ON uif.user_id = u.user_id
JOIN device_dim d ON uif.device_id = d.device_id
WHERE u.age_group = '18-24' 
GROUP BY d.device_type
ORDER BY usage_count DESC;










