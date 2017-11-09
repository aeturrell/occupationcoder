# occupation-coder:
## A tool to associate job text with standard occupational classifications
Given a job title, job description, and job sector the algorithm assigns a 3-digit standard occupational classification (SOC) code to the job. The algorithm uses the SOC 2010 standard, which can be found at WEBSITE.

This code originally written by Jyldyz Djumalieva, Arthur Turrell, and David Copple.

### Testing
The test matches to SOC are run on a file of example jobs, in this case job vacancies.


### Possible extensions/To do
- Put any repeated code into functions which are run efficiently
- Option to use title alone, or just title and description
- Option to output a confidence score
- Move code over to being object oriented so that an occupation-coder instance loads up all of the requisite dictionaries and can then accept either all records at once, or successively.
- Storing the outcomes of successfully matched records within an instance (up to a limit) so that if the same records appear again they get matched more efficiently.
- Make the parallel options scalable (so that they are enabled with a single option, or even automatically)
- Make sure code complies with PEP/usual Python naming conventions
- Review which parts of the code are specific to SOC 2010 (and can be kept fixed) and which parts are specific to the original dataset of vacancies (which need to be changed)
- Tidy up dictionaries
- Set up the tests
- Add more jobs to the test file
