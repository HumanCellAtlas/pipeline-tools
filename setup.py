from setuptools import setup

setup(name='pipeline-tools',
      use_scm_version=True,
      setup_requires=['setuptools_scm'],
      description='Utilities for retrieving files from the HCA data storage service and submitting an analysis bundle to HCA-DCP',
      url='http://github.com/HumanCellAtlas/skylab',
      author='Human Cell Atlas Data Coordination Platform Mint Team',
      author_email='mintteam@broadinstitute.org',
      license='BSD 3-clause "New" or "Revised" License',
      packages=['pipeline_tools'],
      install_requires=[
          'requests>=2.18.4',
          'mock>=2.0.0',
          'google-cloud-storage>=1.8.0',
          'requests-mock>=1.4.0',
          'tenacity>=4.10.0',
          'cromwell-tools',
          'setuptools_scm==2.0.0'
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
      dependency_links=[
            'git+git://github.com/broadinstitute/cromwell-tools.git@v0.3.0#egg=cromwell-tools-1.0.1'
      ],
      include_package_data=True
      )
