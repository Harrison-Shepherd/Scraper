import logging

class DatabaseHelper:
    def __init__(self, connection):
        self.connection = connection

    def get_table_columns(self, table_name):
        """Fetch column names from a given database table."""
        try:
            cursor = self.connection.cursor()
            query = f"SHOW COLUMNS FROM {table_name}"
            cursor.execute(query)
            columns = [column[0] for column in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error fetching columns for table {table_name}: {e}")
            return []
        finally:
            cursor.close()  # Ensure cursor is closed even if there's an error
        return columns

    def insert_data_dynamically(self, table_name, data_dict, json_fields):
        """
        Insert or update data dynamically into the table by matching fields between data and table.
        Parameters:
            table_name (str): Name of the table to insert data into.
            data_dict (dict): Dictionary containing data to be inserted.
            json_fields (dict): Dictionary containing 'required_fields' and 'optional_fields'.
        """
        try:
            # Extract 'required_fields' and 'optional_fields' from the JSON fields
            required_fields = json_fields.get('required_fields', [])
            optional_fields = json_fields.get('optional_fields', [])

            # Combine required and optional fields
            available_fields = required_fields + optional_fields

            # Get the actual table columns from the database
            columns = self.get_table_columns(table_name)

            if not columns:
                logging.error(f"Table {table_name} does not exist or has no columns.")
                return

            # Find which fields from the data_dict can be inserted into the table (matching columns)
            matched_fields = [field for field in available_fields if field in columns]

            # Remove duplicates while preserving order
            seen = set()
            matched_fields = [x for x in matched_fields if not (x in seen or seen.add(x))]

            # Extract the values for the matched fields, ensuring missing fields are handled appropriately
            values = []
            for field in matched_fields:
                value = data_dict.get(field, None)
                
                # Handle None or missing values differently based on the expected data type
                if isinstance(value, (int, float)) or value is None:
                    # If the value is None or numeric, leave it as None (SQL will treat it as NULL)
                    value = value if value is not None else None
                else:
                    # If the value is a string, use an empty string for missing string fields
                    value = value if value != '' else ''
                
                values.append(value)

            # Prepare SQL placeholders and the query
            placeholders = ', '.join(['%s'] * len(matched_fields))
            columns_formatted = ', '.join(matched_fields)

            # Prepare the ON DUPLICATE KEY UPDATE part
            # Exclude primary keys from the update statement to avoid issues
            primary_keys = self.get_primary_keys(table_name)
            update_fields = [field for field in matched_fields if field not in primary_keys]

            if update_fields:
                update_clause = ', '.join([f"{field}=VALUES({field})" for field in update_fields])
                query = f"""
                INSERT INTO {table_name} ({columns_formatted}) 
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
                """
            else:
                # If there are no fields to update, perform a simple insert
                query = f"INSERT IGNORE INTO {table_name} ({columns_formatted}) VALUES ({placeholders})"

            # Logging the SQL query for debugging purposes (optional, can comment out if not needed)
            logging.debug(f"Executing query: {query} with values: {values}")

            cursor = self.connection.cursor()
            cursor.execute(query, values)
            self.connection.commit()

        except Exception as e:
            logging.error(f"Error inserting data into {table_name}: {e}. Data: {data_dict}")
            self.connection.rollback()  # Rollback in case of any error
        finally:
            cursor.close()  # Ensure cursor is closed even if an error occurs

    def get_primary_keys(self, table_name):
        """Retrieve the primary key columns of a table."""
        try:
            cursor = self.connection.cursor()
            query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND CONSTRAINT_NAME = 'PRIMARY'
            """
            cursor.execute(query, (table_name,))
            primary_keys = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error fetching primary keys for table {table_name}: {e}")
            return []
        finally:
            cursor.close()  # Ensure cursor is closed even if an error occurs
        return primary_keys
