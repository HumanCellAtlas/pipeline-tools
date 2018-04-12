from setuptools import setup

setup(name='pipeline-tools',
      version='1.0.0.dev1',
      description='Utilities for retrieving files from the HCA data storage service and submitting an analysis bundle to HCA-DCP',
      url='http://github.com/HumanCellAtlas/skylab',
      author='Human Cell Atlas Data Coordination Platform Mint Team',
      author_email='mintteam@broadinstitute.org',
      license='BSD 3-clause "New" or "Revised" License',
      packages=['pipeline_tools'],
      install_requires=[
          'requests==2.18.4',
          'boto3==1.6.6',
          'mock==2.0.0',
          'google-cloud-storage==1.8.0',
          'requests-mock==1.4.0'

      ],
      entry_points={
          "console_scripts": [
              'get-analysis-metadata=pipeline_tools.get_analysis_metadata:main',
              'create-analysis-json=pipeline_tools.create_analysis_json:main',
              'create-envelope=pipeline_tools.create_envelope:main',
              'get-staging-urn=pipeline_tools.get_staging_urn:main',
              'confirm-submission=pipeline_tools.confirm_submission:main'
          ]
      },
      include_package_data=True
      )
