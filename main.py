# from ser
from crawler import Udemy

if __name__ == '__main__':
    udemy = Udemy()
    all_courses = udemy.crawl()

    # Optional Step for saving courses in elasticsearch
    udemy.save_courses(index_name='udemy_courses')
