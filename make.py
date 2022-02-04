import zipfile

from subprocess import call


def main():
    call('mvn.cmd clean package')
    call('pandoc README.md -o README.pdf')
    with zipfile.ZipFile('target/aggrez.zip', 'w') as zipf:
        zipf.write('README.pdf')
        zipf.write('target/aggrez.jar', 'aggrez.jar')


if __name__ == '__main__':
    main()
