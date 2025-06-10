1. 
Install PostgreSQL
and timescaledb (read here https://docs.timescale.com/self-hosted/latest/install/installation-linux/)

2.
Create a database

<code>sudo -u postgres psql
create user <insert user> with password '<insert password>' superuser;
create database <insert name> owner <insert user>;
\q</code>

3. 
Connect to the db
<code>psql -U <user> -h localhost -p 5432 -d <db name></code>
and execute
<code>CREATE EXTENSION IF NOT EXISTS timescaledb;</code>
Check if the extension was installed successfully
<code>\dx</code>
and quit
<code>\q</code>

5.
Check if there is an operation similar to below in the migrations 0001 for DfReading and DsReading
<code>
        migrations.RunSQL(
            """
            SELECT create_hypertable('df_readings', 'time');
            """,
            reverse_sql="""
                DROP TABLE df_readings;
            """
        ),
</code>

6.
<code>
python manage.py makemigrations
python manage.py migrate
</code>

if you see an error while applying migrations
<code>django constraint unique_name_device of relation does not exist</code>
then follow the approach in the answer here
https://stackoverflow.com/questions/47585826/django-unable-to-migrate-postgresql-constraint-x-of-relation-y-does-not-exist

6.
Create django superuser

7.
Launch
<code>
export MONAPP_PROC_NAME=main
python manage.py runserver --noreload
</code>

9.
Go to the admin page.

Create Intervals - 10 s and 1 min.

Create a Periodic Task named "App 1 Task" with 1 min Interval. Keep enabled=True.

Create an App Type

Create an Application of App Type. Cursor timestamp should be rounded to t_resample. Connect to Intervals. Connect to the Periodic Task. Keep Application is_enabled=True.

Create a Device.

Create Datastreams.

Create Datafeeds.


11.
Set up Mosquitto

go to the file
<code>/etc/mosquitto/conf.d/default.conf</code>

put there

<code>
listener 1883
listener 8083
protocol websockets
socket_domain ipv4
allow_anonymous true
</code>


10.
Launch different terminals

<code>
export MONAPP_PROC_NAME=evaluate
python -m celery -A monapps worker -Q evaluate -E -l info --pool=threads
</code>
<code>
export MONAPP_PROC_NAME=update
python -m celery -A monapps worker -Q update -E -l info --pool=threads
</code>
<code>
export MONAPP_PROC_NAME=beat
python -m celery -A monapps beat -l info -S django
</code>
<code>
export MQTT_SUB_TOPIC="rawdata/<location>/<sublocation>/#"
export MONAPP_PROC_NAME=mqtt_sub
python manage.py run_mqtt_sub
</code>
