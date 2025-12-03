# Two Day Review Practice - ETL Pipeline

Data Engineering practice project covering the full data pipeline workflow.

## Project Overview
This project demonstrates:
- API data extraction
- Data storage in Minio and PostgreSQL
- Data transformation and movement between systems
- AWS S3 integration
- Lambda functions
- Docker containerization

## Technologies Used
- Python 3.x
- PostgreSQL
- Minio (S3-compatible storage)
- AWS S3
- Docker & Docker Compose
- pandas, boto3, psycopg2, minio

## Setup Instructions

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd two-day-review-practice
```

### 2. Create virtual environment
```bash
python -m venv venv
.\venv\Scripts\Activate.ps1  # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up environment variables
Create a `.env` file with your credentials (see `.env.example`)

### 5. Start Docker containers
```bash
docker-compose up -d
```

## Data Flow
1. Fetch data from API → Local file
2. Upload to Minio source bucket
3. Transform data (rename columns) → Minio target bucket
4. Load into PostgreSQL
5. Export to AWS S3
6. Download from S3 back to Minio

## Author
Jing You - Data Engineering Student

## Recent Updates
- Added complete ETL pipeline
- Integrated Docker, Minio, Postgres, and AWS S3
- Created Lambda functions for API endpoints

## Recent Updates
- Added complete ETL pipeline
- Integrated Docker, Minio, Postgres, and AWS S3
- Created Lambda functions for API endpoints
- Practiced Git workflow with branching and PR