<h1>E-Commerce Data Lakehouse with Delta Lake</h1>



<h2>Overview</h2>



<p>

This project implements a <strong>Data Lakehouse architecture</strong> for an e-commerce

analytics platform using <strong>Apache Spark</strong>,

<strong>Delta Lake</strong>, and <strong>MinIO</strong>.

</p>



<p>

It demonstrates how modern data platforms combine the scalability of

data lakes with the reliability and transactional guarantees of

data warehouses.

</p>



<div class="highlight">

The pipeline supports both <strong>batch</strong> and <strong>streaming</strong> workloads

with full ACID guarantees on object storage.

</div>



<p>The solution demonstrates:</p>



<ul>

&nbsp;   <li>Batch ingestion</li>

&nbsp;   <li>Streaming ingestion</li>

&nbsp;   <li>ACID transactions</li>

&nbsp;   <li>Schema enforcement</li>

&nbsp;   <li>Upserts using MERGE</li>

&nbsp;   <li>Time travel queries</li>

&nbsp;   <li>Data optimization and cleanup</li>

</ul>



<h2>Architecture</h2>



<h3>Components</h3>

<ul>

&nbsp;   <li><strong>Apache Spark</strong> – Distributed data processing engine</li>

&nbsp;   <li><strong>Delta Lake</strong> – Storage layer providing ACID transactions and versioning</li>

&nbsp;   <li><strong>MinIO</strong> – S3-compatible object storage</li>

&nbsp;   <li><strong>Docker \& Docker Compose</strong> – Containerized execution environment</li>

</ul>



<h3>Data Flow</h3>

<ol>

&nbsp;   <li>Raw CSV files are uploaded to MinIO (<code>s3a://data/raw/</code>)</li>

&nbsp;   <li>Batch ingestion loads products and customers into Delta tables</li>

&nbsp;   <li>Customer updates are applied using <strong>MERGE</strong> (upsert)</li>

&nbsp;   <li>Time travel queries validate historical data</li>

&nbsp;   <li><code>OPTIMIZE</code> and <code>VACUUM</code> improve performance and clean old files</li>

&nbsp;   <li>Sales data is ingested using Structured Streaming into Delta tables</li>

</ol>



<h2>Project Structure</h2>



<pre>

.

├── Dockerfile

├── docker-compose.yml

├── .env.example

├── README.html

├── requirements.txt

├── app/

│   └── main.py

└── data/

&nbsp;   ├── products.csv

&nbsp;   ├── customers.csv

&nbsp;   ├── updates.csv

&nbsp;   └── sales.csv

</pre>



<h2>Prerequisites</h2>



<ul>

&nbsp;   <li>Docker</li>

&nbsp;   <li>Docker Compose</li>

</ul>



<h2>Environment Variables</h2>



<p>

Create a <code>.env</code> file based on <code>.env.example</code> if needed:

</p>



<pre>

MINIO\_ACCESS\_KEY=minioadmin

MINIO\_SECRET\_KEY=minioadmin

SPARK\_MASTER\_URL=spark://spark-master:7077

</pre>



<h2>How to Run</h2>



<h3>1. Build the Containers</h3>

<pre>

docker-compose build --no-cache

</pre>



<h3>2. Run the Pipeline</h3>

<pre>

docker-compose up

</pre>



<p>This will:</p>



<ul>

&nbsp;   <li>Start Spark and MinIO</li>

&nbsp;   <li>Create required object storage buckets</li>

&nbsp;   <li>Perform batch ingestion</li>

&nbsp;   <li>Apply MERGE upserts</li>

&nbsp;   <li>Run time travel queries</li>

&nbsp;   <li>Optimize and vacuum Delta tables</li>

&nbsp;   <li>Ingest streaming sales data</li>

</ul>



<h2>Verification</h2>



<ul>

&nbsp;   <li>Spark UI: <code>http://localhost:8080</code></li>

&nbsp;   <li>MinIO UI: <code>http://localhost:9001</code></li>

</ul>



<p>

Verify that Delta tables exist under:

</p>



<pre>

s3a://data/warehouse/

</pre>



<p>

Check <code>\_delta\_log</code> directories to confirm Delta Lake operations.

</p>



<h2>Features Demonstrated</h2>



<ul>

&nbsp;   <li>ACID transactions using Delta Lake</li>

&nbsp;   <li>Schema enforcement and partitioning</li>

&nbsp;   <li>MERGE (upsert) operations</li>

&nbsp;   <li>Time travel queries</li>

&nbsp;   <li><code>OPTIMIZE</code> with ZORDER</li>

&nbsp;   <li><code>VACUUM</code> cleanup</li>

&nbsp;   <li>Structured Streaming ingestion</li>

</ul>



<h2>Conclusion</h2>



<div class="success">

This project demonstrates a complete, production-style

<strong>data lakehouse implementation</strong> using open-source technologies,

closely mirroring real-world data engineering workflows.

</div>



