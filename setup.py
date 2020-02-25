"""setup.py for phyllo package."""
import subprocess

import setuptools
from setuptools.command.test import test as testcommand


PACKAGE_NAME = 'phylline'


# CUSTOM TEST COMMANDS


class TestQuickCommand(testcommand):
    """setup.py command to run a quick test prioritizing unstaged changes."""

    description = 'Quick test for unstaged changes and previous failures.'

    def run_tests(self):
        """Run quick unstaged change test."""
        subprocess.check_call([
            'pytest',
            '-n', 'auto',
            '--picked=first',
            '--failed-first'
        ])


class TestQuickBranchCommand(testcommand):
    """setup.py command to run a quick test prioritizing branch changes."""

    description = 'Quick test for branch changes and previous failures.'

    def run_tests(self):
        """Run quick branch change test."""
        subprocess.check_call([
            'pytest',
            '-n', 'auto',
            '--picked=first',
            '--mode=branch',
            '--failed-first'
        ])


class TestDebugCommand(testcommand):
    """setup.py command to run a test and switch to pdb on the first failure."""

    description = 'Quick test for pdb on the first failure.'

    def run_tests(self):
        """Run debugging test."""
        subprocess.check_call([
            'pytest',
            '-n', 'auto',
            '--picked=first',
            '--failed-first',
            '--pdb', '-x'
        ])


class TestPerformanceCommand(testcommand):
    """setup.py command to run a performance profiling test."""

    description = 'Test with performance profiling.'

    def run_tests(self):
        """Run performance profiling test."""
        subprocess.check_call([
            'pytest',
            '--profile', '--profile-svg'
        ])


class TestCoverageCommand(testcommand):
    """setup.py command to run a coverage reporting test."""

    description = 'Test with coverage reporting.'

    def run_tests(self):
        """Run coverage reporting test."""
        subprocess.check_call([
            'pytest',
            '--cov={}'.format(PACKAGE_NAME),
            '--cov-branch',
            '--cov-report', 'term-missing:skip-covered',
            '--cov-report', 'html'
        ])


class TestCompleteCommand(testcommand):
    """setup.py command to run a complete reporting test."""

    description = 'Test with complete reporting.'

    def run_tests(self):
        """Run coverage reporting test."""
        subprocess.check_call([
            'pytest',
            '--durations=0',
            '--cov={}'.format(PACKAGE_NAME),
            '--cov-branch',
            '--cov-report', 'term-missing',
            '--cov-report', 'html',
            '--hypothesis-show-statistics',
            '--verbose'
        ])


# MAIN SETUP

with open('README.md', 'r') as f:
    long_description = f.read()

setuptools.setup(
    name=PACKAGE_NAME,
    version='0.1.5',
    description='Sans-I/O bidirectional data pipeline framework for making layered protocols.',
    url='https://github.com/ethanjli/phylline',
    author='Ethan Li',
    author_email='lietk12@gmail.com',
    packages=setuptools.find_packages(),
    install_requires=[
        'ohneio'
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest', 'hypothesis'],
    cmdclass={
        'test_quick': TestQuickCommand,
        'test_quick_branch': TestQuickBranchCommand,
        'test_debug': TestDebugCommand,
        'test_perf': TestPerformanceCommand,
        'test_coverage': TestCoverageCommand,
        'test_complete': TestCompleteCommand
    }
)
