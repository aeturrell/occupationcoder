# occupation-coder

## A tool to use job text, such as job description, to assign standard occupational classification codes

Given a job title, job description, and job sector the algorithm assigns a 3-digit standard occupational classification (SOC) code to the job. The algorithm uses the SOC 2010 standard, which can be found on [the ONS' website](https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc/soc2010).

This code originally written by Jyldyz Djumalieva, Arthur Turrell, and David Copple. If you use this code please cite:
"Pretty Vacant: Understanding mismatch in the UK labour market from the bottom-up"

### Running the code
If you have all the relevant packages, navigate to the occupation-coder/Coder directory and run
```
python multiprocessing_cleaning_and_processing.py path/to/file/foo.csv
```
This will create a 'processed_jobs.csv' file in Outputs/ which has the original text and an extra 'SOC_code' column with assigned SOC codes.

### Testing
The test matches to SOC are run on a file of example jobs, in this case job vacancies.
The code to run the test is
```
python multiprocessing_cleaning_and_processing.py ../TestVacancies/test_vacancies.csv
```

### To do
- Put any repeated code into functions which are run efficiently
- Option to use title alone, or just title and description
- Option to output a confidence score
- Move code over to being object oriented so that an occupation-coder instance loads up all of the requisite dictionaries and can then accept either all records at once, or successively.
- Storing the outcomes of successfully matched records within an instance (up to a limit) so that if the same records appear again they get matched more efficiently.
- Make the parallel options scalable (so that they are enabled with a single option, or even automatically)
- Make sure code complies with PEP/usual Python naming conventions
- Add more jobs to the test file
