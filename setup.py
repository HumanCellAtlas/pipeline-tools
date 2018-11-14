from setuptools import setup


setup(name='pipeline-tools',
      use_scm_version=True,
      setup_requires=['setuptools_scm'],
      classifiers=[
          'Programming Language :: Python :: 3',
      ],
      description='Utilities for retrieving files from the HCA data storage service and submitting an analysis bundle '
                  'to HCA-DCP',
      url='http://github.com/HumanCellAtlas/skylab',
      author='Human Cell Atlas Data Coordination Platform Mint Team',
      author_email='mintteam@broadinstitute.org',
      license='BSD 3-clause "New" or "Revised" License',
      packages=['pipeline_tools'],
      install_requires=[
          'cromwell-tools',
          'google-cloud-storage>=1.8.0,<2',
          'hca>=4.4.9,<5',
          'hca-metadata-api',
          'mock>=2.0.0,<3',
          'requests>=2.20.0,<3',
          'requests-mock>=1.5.2,<2',
          'setuptools_scm>=2.0.0,<3',
          'tenacity>=4.10.0,<5',
          'google-cloud-storage>=1.10.0,<2',
      ],
      entry_points={
          'console_scripts': [
              'get-analysis-workflow-metadata=pipeline_tools.get_analysis_workflow_metadata:main',
              'create-analysis-metadata=pipeline_tools.create_analysis_metadata:main',
              'create-envelope=pipeline_tools.create_envelope:main',
              'get-upload-urn=pipeline_tools.get_upload_urn:main',
              'get-files-to-upload=pipeline_tools.get_files_to_upload:main',
              'confirm-submission=pipeline_tools.confirm_submission:main'
          ]
      },
      # FIXME: DEPRECATION: Dependency Links processing has been deprecated and will be removed in a future release.
      dependency_links=[
          'git+git://github.com/broadinstitute/cromwell-tools.git@v0.5.0#egg=cromwell-tools-1.0.1',
          # FIXME: install hca-metadata-api from PyPI once it is available (shortly)
          # Pin to a specific commit of the hca-metadata-api so we won't be broken by changes to that repo before it's
          # available on PyPI
          'git+git://github.com/HumanCellAtlas/metadata-api@release/1.0b4#egg=hca-metadata-api-0.0.1'
      ],
      include_package_data=True
      )
