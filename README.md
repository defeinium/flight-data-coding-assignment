# Flight Analytics Project

## Description

This project analyzes flight data to answer 4 analytical questions, including:

- **Monthly Flight Statistics:** Q1 - Find the total number of flights for each month.
- **Top Frequent Flyers:** Q2 - Find the names of the 100 most frequent flyers
- **Longest Runs Without Visiting a Specific Country:** Q3 - Find the greatest number of countries a passenger has been in without being in the UK.
- **Passengers Flown Together Frequently:** Q4 - Find the passengers who have been on more than 3 flights together, and find the passengers who have been on more than N flights together within the range (from,to).

## Requirements

- **Scala 2.12.10**
- **SBT** (Scala Build Tool)
- **Apache Spark 2.4.8**
- **Java 8** or later

## How to Run

### Steps

1. **Clone the Repository or Extract the ZIP File**

   - If cloning from GitHub:
     ```bash
     cd to the path you wish
     git clone https://github.com/defeinium/flight-data-coding-assignment.git
     
     ```
   - If using the ZIP file:
     ```bash
     unzip project_submission.zip
     cd project_directory
     ```

2. **Compile and Run the Project**

   - Compile the project:
     ```bash
     sbt compile
     ```
   - Run the application:
     ```bash
     sbt run
     ```

3. **Provide Input When Prompted**

   - Follow the on-screen prompts for user input (e.g., minimum number of flights together and date ranges).

4. **View Output Files**

   - Results are saved as CSV files in the `output` directory.

## Outputs

The application generates the following output files:

- `flights_by_month.csv`: Q1.
- `top_flyers.csv`: Q2.
- `longest_run.csv`: Q3.
- `flown_together.csv`: Q4.

## CSV Files

Sample results are included in the `output` folder. Each file corresponds to one of the key questions addressed by the project.

## Notes

- Ensure all dependencies (Scala, SBT, Java) are installed before running the project.
- For any issues, contact defeinium@gmail.com.

