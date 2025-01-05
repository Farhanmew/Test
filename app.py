from flask import Flask, request, render_template, send_file
import os
from werkzeug.utils import secure_filename
import pandas as pd

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    file1 = request.files['file1']
    file2 = request.files['file2']

    if not file1 or not file2:
        return "Both files are required!", 400

    file1_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file1.filename))
    file2_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file2.filename))

    file1.save(file1_path)
    file2.save(file2_path)

    # Process the files using the script logic
    processed_file_path = "final_processed_file.csv"
    try:
        process_and_merge_files(file1_path, file2_path, processed_file_path)
    except Exception as e:
        return f"Error: {str(e)}", 500

    return send_file(processed_file_path, as_attachment=True)

def process_and_merge_files(file1_path, file2_path, output_path):
    # Load both files
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)
 
    # Step 2: Remove unnecessary rows from the first file
    df1.drop(index=[0, 1], inplace=True)

    # Step 3: Extract specific columns from the first file
    columns_to_extract1 = ['Q4', 'Q9#1_1', 'Q9#1_2', 'Q9#1_3', 'Q9#1_4',
                           'Q9#1_5', 'Q9#1_6', 'Q9#1_7', 'Q9#1_8', 'Q9#1_9',
                           'Q9#1_10', 'Q9#1_11', 'Q9#1_12']
    
    extracted_df1 = df1[columns_to_extract1]

    # Step 4: Rename columns for clarity in the first file
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

  

    # Step 5: Map ratings to numerical values for the first file
    rating_to_score = {
        'Poor': 1,
        'Average': 2,
        'Good': 3,
        'Very Good': 4,
        'Excellent': 5
    }

    # Apply mapping and handle NaNs
    for column in extracted_df1.columns[1:]:
        extracted_df1[column] = extracted_df1[column].map(rating_to_score).fillna(0)

    # Step 6: Add a column with the sum of rows for the first file
    extracted_df1['Total Score'] = extracted_df1.iloc[:, 1:].sum(axis=1)


    # Step 9: Extract specific columns from the second file
    columns_to_extract2 = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
    
    # Check if the columns exist in df2
    extracted_df2 = df2[columns_to_extract2]

    # Debugging: Check data after extraction

    # Step 10: Drop unnecessary rows from the second file
    #extracted_df2.drop(index=[1], inplace=True)

    # Step 11: Rename column to align with the first file's 'Full Name'
    extracted_df2.rename(columns={'Q1': 'Full Name'}, inplace=True)

    # Debugging: Check the final merged data
   
    final_df = pd.merge(extracted_df1[['Full Name', 'Total Score']], extracted_df2, on='Full Name', how='inner')



    # Step 12: Save the final merged data to a new CSV file
    final_output_file = "final_processed_file.csv"
    final_df.to_csv(final_output_file, index=False)
    print(f"Final processed file saved as {final_output_file}")



if __name__ == '__main__':
    app.run(debug=True)