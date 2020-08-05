from distutils.core import setup
setup(
  name = 'PyFutureOps',         # How you named your package folder (MyLib)
  packages = ['PyFutureOps'],   # Chose the same as "name"
  version = '0.1',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Bootstrap code for Concurrent Futures Batch tracking',   # Give a short description about your library
  author = 'Kunal Gupta',                   # Type in your name
  author_email = 'rushtokunal@gmail.com',      # Type in your E-Mail
  url = 'https://github.com/rushtokunal/PyFutureOps',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/rushtokunal/PyFutureOps/archive/v0.1.tar.gz',
  keywords = ['concurrent futures', 'multithreading', 'multiprocessing', 'spanner', 'restart recovery'],   # Keywords that define your package best
  install_requires=[            
          'google-cloud-spanner',
          'pandas',
          'requests'
      ],
  classifiers=[
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3.7',      #Specify which pyhton versions that you want to support
  ],
)