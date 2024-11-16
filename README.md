## Prerequisites

1. Python 3.12 or higher
2. venv


## Setup

1. Create a virtual environment: python -m venv venv
2. Activate the virtual environment: source venv/bin/activate
3. Install dependencies: pip install -r requirements.txt


## Design rationale

1. Using sqlite3. For this exercise, I'm trying to keep to keep to minimal
   external dependencies, as much as possible using standard library. In
   production ready application, I'd have chosen a more proper database system
   like Postgres and would have setup a Docker setup, but I'm not quite sure if
   that will be available on the interviewer's system.

2. Money type with Decimal. In production application, I'd have used a Money
   class to handle currencies explicitly in the system.

3. The exercise does not require cryptographic tamperproofing of the
   transaction log, but I thought it'd be of interest to implement this anyway
   due to our discussion.

4. Use of requirements.txt. Not the most modern practice, I would have used
   poetry for actual projects, but it's simplest for the purpose of this
   assignment.
