import requests
from utilities import ElasticWrapper
from datetime import datetime


class Udemy:
    def __init__(self):
        self.already_seen_courses = set()
        self.base_url = 'https://www.udemy.com/api-2.0/discovery-units/all_courses/' + \
                        '?page_size=60&source_page=category_page&locale=en_US&' + \
                        'navigation_locale=en_US&skip_price=true&sos=pc&fl=cat'
        self.category_mapping = {
            'development': 288,
            'business': 268,
            'finance': 328,
            'software': 294,
            'productivity': 292,
            'personal_development': 296,
            'design': 269,
            'health_fitness': 276,
            'marketting': 290,
            'lifestyle': 274,
            'photography': 273,
            'music': 278,
            'teaching': 300
        }

        self.extracted_features = [
            'id',
            'title',
            'url',
            'is_paid',
            'visible_instructors',
            'image_480x270',
            'published_title',
            'headline',
            'num_subscribers',
            'avg_rating',
            'avg_rating_recent',
            'rating',
            'num_reviews',
            'num_published_lectures',
            'num_published_practice_tests',
            'has_closed_caption',
            'created',
            'instructional_level',
            'published_time',
            'objectives_summary',
            'is_recently_published',
            'last_update_date',
            'content_info',
        ]
        self.all_courses = []
        self.category_courses = {}

    def crawl(self):
        for category in self.category_mapping:
            print(f'********* STARTING CATEGORY {category} ************\n\n')
            category_courses = self.crawl_category(category)
            self.save_category(category)
            self.all_courses.extend(category_courses)
            print(f'\n\n********* ENDING CATEGORY {category} ************\n\n')

        return self.all_courses

    def crawl_category(self, category):
        category_code = self.category_mapping[category]
        page_number = 1
        self.category_courses[category] = []

        while True:
            print(f'starting page {page_number} of category {category}')

            url = self.get_full_url(page_number, category_code)
            resp = requests.get(url)
            if resp.status_code != 200:
                return self.category_courses[category]

            courses = resp.json()['unit']['items']

            courses = [course for course in courses if course['id'] not in self.already_seen_courses]

            self.already_seen_courses.update(course['id'] for course in courses)

            self.category_courses[category].extend([{
                **self.extract_features(course),
                'category': category,
                'created_datetime': datetime.now()
            } for course in courses])

            page_number += 1

    def extract_features(self, course):
        return {k: course[k] for k in self.extracted_features}

    def get_full_url(self, page_number, category_id):
        return f'{self.base_url}&p={page_number}&category_id={category_id}'

    def save_category(self, category, *, index_name='udemy_courses'):
        courses = self.category_courses[category]
        return ElasticWrapper.bulk_index_docs(courses, index_name=index_name)

    def save_courses(self, *, index_name='udemy_courses'):
        return ElasticWrapper.bulk_index_docs(self.all_courses, index_name=index_name)


__all__ = ['Udemy']
