import random

def has_streak(lst):
    """Check if there is a streak of 6 identical elements in the list"""
    for i in range(len(lst) - 5):
        if all(lst[i+j] == lst[i] for j in range(6)):
            return True
    return False

def main():
    num_experiments = 10000
    num_streaks = 0
    for i in range(num_experiments):
        flips = [random.randint(0, 1) for _ in range(100)]
        if has_streak(flips):
            num_streaks += 1
    percentage = num_streaks / num_experiments * 100
    print(f"Percentage of experiments with a streak of 6 heads or tails: {percentage}%")

if __name__ == "__main__":
    main()
