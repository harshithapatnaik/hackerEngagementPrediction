# Importing necessary libraries
import psycopg2
import matplotlib.pyplot as plt

# Establishing connection to the PostgreSQL database
conn = psycopg2.connect(
dbname= "darkweb_markets_forums2",
user="postgres",
password="#arshly4P",
host="localhost",
port="5432"
)

# Creating a cursor object
cursor = conn.cursor()

forum_id = 2
unique_user_requirement = 10

# SQL query to retrieve data
query = f"select p.user_id, count(p.post_id), count(distinct p.topic_id) from posts p inner join topics t on t.topic_id = p.topic_id " \
        f"where t.forum_id = {forum_id} and length(content_post) > 10 and classification_topic >= 0.5 " \
        f"group by p.user_id"

# Executing the query
cursor.execute(query)

# Fetching all the rows
rows = cursor.fetchall()
print(rows)
count_dict = {20:0}
for x in rows:
    count = x[2]
    if count >= 3:
        if count >= 20:
            count_dict[20] += 1
        elif count not in count_dict:
            count_dict[count] = 1
        else:
            count_dict[count] += 1

cursor.close()
conn.close()

key_strings = []
values = []
for key in sorted(count_dict.keys()):
    if key >= 20:
        key_strings.append(str(key) + "+")
    else:
        key_strings.append(str(key))
    values.append(count_dict[key])

color = "navy"
if forum_id == '77':
    color = "navy"
elif forum_id == '84':
    color = "#a17f1a"  # dark gold
else:
    color = "maroon"

plt.bar(key_strings, values, color=color,
        width=0.4)

plt.xlabel("Amount of threads")
plt.ylabel("Amount of users")
plt.title("Unique thread participation by user for forum " + str(forum_id))
plt.show()