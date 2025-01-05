from flask import Flask, request, render_template, send_file
import os
from werkzeug.utils import secure_filename
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024  # 10 MB file size limit
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    try:
        # Get the uploaded files
        file1 = request.files['file1']
        file2 = request.files['file2']

        if not file1 or not file2:
            return "Both files are required!", 400

        if not file1.filename.endswith('.csv') or not file2.filename.endswith('.csv'):
            return "Both files must be in CSV format!", 400

        # Save files securely
        file1_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file1.filename))
        file2_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file2.filename))
        file1.save(file1_path)
        file2.save(file2_path)

        # Process the files
        processed_file_path = "final_processed_file.csv"
        process_and_merge_files(file1_path, file2_path, processed_file_path)

        # Cleanup uploaded files
        os.remove(file1_path)
        os.remove(file2_path)

        return send_file(processed_file_path, as_attachment=True)

    except Exception as e:
        logging.error(f"Error during processing: {str(e)}")
        return f"Error: {str(e)}", 500

def process_and_merge_files(file1_path, file2_path, output_path):
    try:
        # Load both files
        df1 = pd.read_csv(file1_path)
        df2 = pd.read_csv(file2_path)

        # Validate required columns in file1
        required_columns1 = ['Q4', 'Q9#1_1', 'Q9#1_2', 'Q9#1_3', 'Q9#1_4',
                             'Q9#1_5', 'Q9#1_6', 'Q9#1_7', 'Q9#1_8', 'Q9#1_9',
                             'Q9#1_10', 'Q9#1_11', 'Q9#1_12']
        if not all(col in df1.columns for col in required_columns1):
            raise ValueError("File1 is missing required columns.")

        # Process file1
        df1.drop(index=[0, 1], inplace=True)
        extracted_df1 = df1[required_columns1]
        extracted_df1.rename(columns={
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
        }, inplace=True)

        rating_to_score = {'Poor': 1, 'Average': 2, 'Good': 3, 'Very Good': 4, 'Excellent': 5}
        for column in extracted_df1.columns[1:]:
            extracted_df1[column] = extracted_df1[column].map(rating_to_score).fillna(0)
        extracted_df1['Total Score'] = extracted_df1.iloc[:, 1:].sum(axis=1)

        # Validate required columns in file2
        required_columns2 = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
        if not all(col in df2.columns for col in required_columns2):
            raise ValueError("File2 is missing required columns.")

        # Process file2
        extracted_df2 = df2[required_columns2]
        extracted_df2.rename(columns={'Q1': 'Full Name'}, inplace=True)

        # Merge the processed data
        final_df = pd.merge(extracted_df1[['Full Name', 'Total Score']], extracted_df2, on='Full Name', how='inner')
        final_df.to_csv(output_path, index=False)
        logging.info(f"Final processed file saved as {output_path}")

    except Exception as e:
        raise ValueError(f"Error processing files: {str(e)}")

if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=5000)
