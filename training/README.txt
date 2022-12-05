Hi, there are some assumptions/changes when making this project:
1. American refers to when the only country listed is the US
2. Duration was used instead of runtime because that was the name that all files shared
3. Duration and date_format had the same formats across all files so there was no need to check the format
3. Tests were done on FilmManager instead of the luigi tasks because that's where the business logic is. Otherwise, I felt that I would be testing if luigi itself was working.

Also the main programs are run in the training directory whereas the tests are run in the test directory.
