from distutils.core import setup

def main():
    setup(
        name='jsqlon',
        description='Sync your SQLite with a JSON backup.',
        version='1.0',
        packages=['jsqlon'],
    )

if __name__ == "__main__":
    main()
