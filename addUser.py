import airflow
import getpass
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


def validateNotEmpty(userInput, fieldName):
    if len(userInput) == 0:
        print(fieldName, ' cannot be empty')
        exit()


username = getpass._raw_input('Username: ')
validateNotEmpty(username, 'Username')

email = getpass._raw_input('Email: ')
validateNotEmpty(email, 'Email')

password = getpass.getpass()
validateNotEmpty(password, 'Password')

repassword = getpass.getpass('Re-type password:')
validateNotEmpty(repassword, 'Re-type password:')

if password <> repassword:
    print('Re-type password should match Password')
    exit()

print('Creating user ...')
user = PasswordUser(models.User())
user.username = username
user.email = email
user.password = password
session = settings.Session()
session.add(user)
session.commit()
session.close()
print('User created!!!')
exit()
