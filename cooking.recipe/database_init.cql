CREATE KEYSPACE cooking WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE cooking.recipe ( 
recipe_id int PRIMARY KEY, 
description text, 
recipe_name text 
); 

CREATE TABLE cooking.recipe_event ( 
recipe_id int,
recipe_name text,
description text,
ingredient text, 
active boolean, 
updated_date timestamp, 
created_date timestamp, 
PRIMARY KEY (recipe_id,ingredient,updated_date)    
) WITH CLUSTERING ORDER BY (ingredient ASC, updated_date DESC);

CREATE TABLE cooking.ingredient_recipe ( 
ingredient text,
recipe_id int,
recipe_name text,
PRIMARY KEY (ingredient,recipe_id)     
); 

CREATE TABLE cooking.recipe_ingredient ( 
recipe_id int,
ingredient text,
active boolean, 
updated_date timestamp,
PRIMARY KEY (recipe_id, ingredient)     
);