import os
import hashlib
import csv
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import timedelta
from flask import Flask, request, render_template, send_file, abort
from werkzeug.utils import secure_filename
import tempfile

# Config class
class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-change-in-production'
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER') or 'uploads'  # Ensure this folder exists
    MAX_CONTENT_LENGTH = 10 * 1024 * 1024  # 10 MB
    ALLOWED_EXTENSIONS = {'csv'}
    SESSION_COOKIE_SECURE = True
    PERMANENT_SESSION_LIFETIME = timedelta(minutes=30)
    LOG_FILE = 'app.log'

# Set up logging
def setup_logging(app):
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    file_handler = RotatingFileHandler(
        'logs/app.log', maxBytes=1024 * 1024, backupCount=10)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')
    )
    file_handler.setLevel(logging.INFO)
    app.logger.addHandler(file_handler)
    app.logger.setLevel(logging.INFO)
    app.logger.info('Application startup')

# Create the app
def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Initialize CSRF protection
    
    
    # Ensure upload folder exists
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    setup_logging(app)
    
    return app

# Initialize app
app = create_app()

# Allowed file extensions check
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

# Validate CSV structure
def validate_csv_structure(file_path, required_columns):
    try:
        with open(file_path, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            return all(col in headers for col in required_columns)
    except Exception as e:
        app.logger.error(f"CSV validation error: {str(e)}")
        return False

# Health check route
@app.route('/health')
def health_check():
    return {'status': 'healthy'}, 200

# Index route
@app.route('/')
def index():
    return render_template('index.html')

# Process files route
@app.route('/process', methods=['POST'])
def process_files():
    if 'file1' not in request.files or 'file2' not in request.files:
        abort(400, description="Both files are required")

    file1 = request.files['file1']
    file2 = request.files['file2']

    # Validate file existence and extension
    for file in [file1, file2]:
        if file.filename == '':
            abort(400, description="No selected file")
        if not allowed_file(file.filename):
            abort(400, description="Invalid file type")

    try:
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save files securely with unique names
            file1_path = os.path.join(temp_dir, secure_filename(
                hashlib.md5(file1.filename.encode()).hexdigest() + '.csv'))
            file2_path = os.path.join(temp_dir, secure_filename(
                hashlib.md5(file2.filename.encode()).hexdigest() + '.csv'))
            
            file1.save(file1_path)
            file2.save(file2_path)

            # Validate CSV structure
            required_columns1 = ['Q4', 'Q9#1_1', 'Q9#1_2', 'Q9#1_3', 'Q9#1_4',
                               'Q9#1_5', 'Q9#1_6', 'Q9#1_7', 'Q9#1_8', 'Q9#1_9',
                               'Q9#1_10', 'Q9#1_11', 'Q9#1_12']
            required_columns2 = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']

            if not validate_csv_structure(file1_path, required_columns1):
                abort(400, description="Invalid structure in file 1")
            if not validate_csv_structure(file2_path, required_columns2):
                abort(400, description="Invalid structure in file 2")

            # Process files
            output_path = os.path.join(temp_dir, 'processed_result.csv')
            process_and_merge_files(file1_path, file2_path, output_path)

            # Send processed file
            return send_file(
                output_path,
                as_attachment=True,
                download_name='processed_result.csv',
                mimetype='text/csv'
            )

    except Exception as e:
        app.logger.error(f"Processing error: {str(e)}")
        abort(500, description=str(e))

# Process and merge files
def process_and_merge_files(file1_path, file2_path, output_path):
    try:
        # Load both files
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)

        # Process file1
        df1.drop(index=[0, 1], inplace=True)
        extracted_df1 = df1[['Q4'] + [f'Q9#1_{i}' for i in range(1, 13)]]
        
        column_mapping = {
            'Q4': 'Full Name',
            'Q9#1_1': 'Technical contribution to field',
            'Q9#1_2': 'Ability to explain complex ideas',
            'Q9#1_3': 'Dependability, Reliability',
            'Q9#1_4': 'Initiative',
            'Q9#1_5': 'Interpersonal skills/Collaboration with others',
            'Q9#1_6': 'Time Management',
            'Q9#1_7': 'Work Ethics',
            'Q9#1_8': 'Response to feedback',
            'Q9#1_9': 'Listening to others',
            'Q9#1_10': 'Professionalism',
            'Q9#1_11': 'Willingness to have responsibility',
            'Q9#1_12': 'Ability to follow instructions'
        }
        
        extracted_df1.rename(columns=column_mapping, inplace=True)

        # Map ratings to scores
        rating_to_score = {
            'Poor': 1,
            'Average': 2,
            'Good': 3,
            'Very Good': 4,
            'Excellent': 5
        }
        
        for column in extracted_df1.columns[1:]:
            extracted_df1[column] = extracted_df1[column].map(rating_to_score).fillna(0)
        
        extracted_df1['Total Score'] = extracted_df1.iloc[:, 1:].sum(axis=1)

        # Process file2
        extracted_df2 = df2[['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']]
        extracted_df2.rename(columns={'Q1': 'Full Name'}, inplace=True)

        # Merge and save
        final_df = pd.merge(
            extracted_df1[['Full Name', 'Total Score']],
            extracted_df2,
            on='Full Name',
            how='inner'
        )
        final_df.to_csv(output_path, index=False)

    except Exception as e:
        app.logger.error(f"Data processing error: {str(e)}")
        raise ValueError(f"Error processing files: {str(e)}")

# requirements.txt
# flask==2.3.3
# pandas==2.1.0
# werkzeug==2.3.7
# flask-wtf==1.1.1
# gunicorn==21.2.0

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
