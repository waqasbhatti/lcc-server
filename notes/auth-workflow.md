# Sign up

- enter email address
- enter password
- confirm password

- limit email address length to between 5 and 255 characters
- limit raw password length to between 12 and 1024 characters

- profile items (copied over from Django):
  - email
  - passlib-hashed password
  - first name
  - last name
  - groups
  - user_permissions
  - is_staff
  - is_active: set when email is confirmed
  - is_superuser
  - datetime_lastlogin
  - datetime_joined

# Confirm email address

- this should not allow people hitting it more than once per day
- use sessions for this


# Redirect to login

# Log in

- this should be rate-limited

# Redirect to index page

# Visit profile page

# View owned items

# Edit owned item permissions (note: nothing can be deleted)

# Change profile items (not password)

# Change password

# Confirm password change by email

- this should be rate-limited to once per day

# Get an API key

- this should be rate-limited to once per day

# Expire API key

# Log out

# Forgot password

# Password recovery confirmation email link

# Delete account

# Delete account confirmation by asking password

# Delete account done
