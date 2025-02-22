import os
import subprocess

# ANSI Escape Codes for Colors
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
RESET = "\033[0m"  # Resets to default terminal color

# This script finds all test files in the project and runs them independently.
# It counts the number of successful tests, failed tests, and tests with errors.
# It checks for test files starting with 'test_' and ending with '.py'.

def find_test_files(start_dir="."):
    """Recursively find all test files starting with 'test_' in the project."""
    test_files = []
    for root, _, files in os.walk(start_dir):
        for file in files:
            if file.startswith("test_") and file.endswith(".py"):
                test_files.append(os.path.join(root, file))
    return test_files

def runTests(start_dir="."):
    """Run test scripts independently and count successes, failures, and errors with colored output."""
    test_files = find_test_files(start_dir)

    if not test_files:
        print(RED + "\n❌ No test files found." + RESET)
        return 1

    total_tests = len(test_files)
    successful_tests = 0
    failed_tests = []
    errored_tests = []

    for test_file in test_files:
        print(CYAN + f"\n🔄 Running {test_file}..." + RESET)

        try:
            result = subprocess.run(["python3", test_file], capture_output=True, text=True, check=True)
            print(GREEN + result.stdout + RESET)  # Show successful test output in green
            successful_tests += 1
        except subprocess.CalledProcessError as e:
            print(YELLOW + e.stdout + RESET)  # Show standard output before failure in yellow
            print(RED + e.stderr + RESET)  # Show error message in red
            failed_tests.append(test_file)
        except Exception as e:
            print(RED + f"⚠️ Unexpected error while running {test_file}: {e}" + RESET)
            errored_tests.append(test_file)

    # Display the test summary
    print(BLUE + "\n==== TEST SUMMARY ====" + RESET)
    print(f"Total Tests Run: {total_tests}")
    print(GREEN + f"✅ Successful: {successful_tests}" + RESET)

    if failed_tests:
        print(RED + f"\n❌ {len(failed_tests)} Test(s) Failed:" + RESET)
        for test in failed_tests:
            print(RED + f"  - {test}" + RESET)

    if errored_tests:
        print(MAGENTA + f"\n⚠️ {len(errored_tests)} Test(s) Encountered Errors:" + RESET)
        for test in errored_tests:
            print(MAGENTA + f"  - {test}" + RESET)

    if successful_tests == total_tests:
        print(GREEN + "\n✅ All tests passed!" + RESET)
        return 0
    else:
        print(RED + "\n❌ Some tests failed or had errors." + RESET)
        return 1

if __name__ == "__main__":
    runTests()
