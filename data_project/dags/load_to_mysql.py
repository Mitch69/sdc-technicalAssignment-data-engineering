from airflow.providers.mongo.hooks.mysql import MySqlHook


def load_data(news_articles_dict):
    # Initialize MySQL hook
    mysql_hook = MySqlHook(mysql_conn_id=8, schema="db")
    conn = mysql_hook.get_sqlalchemy_engine()
    
    # Create the Warehouse schema
    create_date_dimension = """
    CREATE TABLE IF NOT EXISTS date_dimension (
        date DATE PRIMARY KEY,
        year INT,
        month INT,
        day INT
    );
    """
    
    create_author_dimension = """
    CREATE TABLE IF NOT EXISTS author_dimension (
        author_id INT AUTO_INCREMENT PRIMARY KEY,
        author VARCHAR(255)
    );
    """
    
    create_source_dimension = """
    CREATE TABLE IF NOT EXISTS source_dimension (
        source_id INT AUTO_INCREMENT PRIMARY KEY,
        source VARCHAR(255)
    );
    """
    
    create_fact_table = """
    CREATE TABLE IF NOT EXISTS fact_table (
        article_id INT AUTO_INCREMENT PRIMARY KEY,
        newstitle VARCHAR(255),
        description TEXT,
        url_source TEXT,
        urltoimage TEXT,
        content TEXT,
        source_id INT,
        author_id INT,
        date DATE,
        FOREIGN KEY (source_id) REFERENCES source_dimension(source_id),
        FOREIGN KEY (author_id) REFERENCES author_dimension(author_id),
        FOREIGN KEY (date) REFERENCES date_dimension(date)
    );
    """
    
    # Execute SQL to create the tables
    mysql_hook.run(create_date_dimension)
    mysql_hook.run(create_author_dimension)
    mysql_hook.run(create_source_dimension)
    mysql_hook.run(create_fact_table)
    
    
    # Insert into date_dimension
    for _, row in news_articles_dict['date_dim'].iterrows():
        insert_query = """
        INSERT INTO date_dimension (date, year, month, day)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        year=VALUES(year), month=VALUES(month), day=VALUES(day);
        """
        mysql_hook.run(insert_query, parameters=(row['date'], row['year'], row['month'], row['day']))
    
    # Insert into author_dimension
    for _, row in news_articles_dict['author_dim'].iterrows():
        insert_query = """
        INSERT INTO author_dimension (author_id, author)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE 
        author=VALUES(author);
        """
        mysql_hook.run(insert_query, parameters=(row['author_id'], row['author']))
    
    # Insert into source_dimension
    for _, row in news_articles_dict['source_dim'].iterrows():
        insert_query = """
        INSERT INTO source_dimension (source_id, source)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE 
        source=VALUES(source);
        """
        mysql_hook.run(insert_query, parameters=(row['source_id'], row['source']))
    
    # Insert into fact_table
    for _, row in news_articles_dict['fact_table'].iterrows():
        insert_query = """
        INSERT INTO fact_table (author, newstitle, description, url_source, urltoimage, content, source, date, time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        author=VALUES(author), newstitle=VALUES(newstitle), description=VALUES(description),
        url_source=VALUES(url_source), urltoimage=VALUES(urltoimage), content=VALUES(content),
        source=VALUES(source), date=VALUES(date), time=VALUES(time);
        """
        
        mysql_hook.run(insert_query, parameters=(row['article_id'], row['newstitle'], row['date'], row['author_id'], row['source_id']))

    print("Data has been successfully pushed to MySQL.")


