import json
import os

def create_sample_data():
    """Create sample manga data for testing"""
    
    # Sample works
    sample_works = [
        {
            "uri": "http://example.com/work/1",
            "title": "ONE PIECE",
            "creator_uri": "http://example.com/author/1",
            "creator_name": "尾田栄一郎",
            "publisher": "集英社",
            "publication_date": "1997",
            "genre": "少年漫画"
        },
        {
            "uri": "http://example.com/work/2",
            "title": "NARUTO",
            "creator_uri": "http://example.com/author/2", 
            "creator_name": "岸本斉史",
            "publisher": "集英社",
            "publication_date": "1999",
            "genre": "少年漫画"
        },
        {
            "uri": "http://example.com/work/3",
            "title": "BLEACH",
            "creator_uri": "http://example.com/author/3",
            "creator_name": "久保帯人",
            "publisher": "集英社", 
            "publication_date": "2001",
            "genre": "少年漫画"
        },
        {
            "uri": "http://example.com/work/4",
            "title": "DRAGON BALL",
            "creator_uri": "http://example.com/author/4",
            "creator_name": "鳥山明",
            "publisher": "集英社",
            "publication_date": "1984",
            "genre": "少年漫画"
        },
        {
            "uri": "http://example.com/work/5",
            "title": "SLAM DUNK",
            "creator_uri": "http://example.com/author/5",
            "creator_name": "井上雄彦",
            "publisher": "集英社",
            "publication_date": "1990",
            "genre": "少年漫画"
        },
        {
            "uri": "http://example.com/work/6",
            "title": "鬼滅の刃",
            "creator_uri": "http://example.com/author/6",
            "creator_name": "吾峠呼世晴",
            "publisher": "集英社",
            "publication_date": "2016",
            "genre": "少年漫画"
        },
        {
            "uri": "http://example.com/work/7",
            "title": "進撃の巨人",
            "creator_uri": "http://example.com/author/7",
            "creator_name": "諫山創",
            "publisher": "講談社",
            "publication_date": "2009",
            "genre": "少年漫画"
        },
        {
            "uri": "http://example.com/work/8",
            "title": "デスノート",
            "creator_uri": "http://example.com/author/8",
            "creator_name": "大場つぐみ",
            "publisher": "集英社",
            "publication_date": "2003",
            "genre": "少年漫画"
        }
    ]
    
    # Sample authors
    sample_authors = [
        {
            "uri": "http://example.com/author/1",
            "name": "尾田栄一郎",
            "birth_date": "1975-01-01",
            "death_date": "",
            "nationality": "日本"
        },
        {
            "uri": "http://example.com/author/2",
            "name": "岸本斉史", 
            "birth_date": "1974-11-08",
            "death_date": "",
            "nationality": "日本"
        },
        {
            "uri": "http://example.com/author/3",
            "name": "久保帯人",
            "birth_date": "1977-06-26",
            "death_date": "",
            "nationality": "日本"
        },
        {
            "uri": "http://example.com/author/4",
            "name": "鳥山明",
            "birth_date": "1955-04-05",
            "death_date": "",
            "nationality": "日本"
        },
        {
            "uri": "http://example.com/author/5",
            "name": "井上雄彦",
            "birth_date": "1967-01-12",
            "death_date": "",
            "nationality": "日本"
        },
        {
            "uri": "http://example.com/author/6",
            "name": "吾峠呼世晴",
            "birth_date": "1989-05-05",
            "death_date": "",
            "nationality": "日本"
        },
        {
            "uri": "http://example.com/author/7",
            "name": "諫山創",
            "birth_date": "1986-08-29",
            "death_date": "",
            "nationality": "日本"
        },
        {
            "uri": "http://example.com/author/8",
            "name": "大場つぐみ",
            "birth_date": "1970-01-01",
            "death_date": "",
            "nationality": "日本"
        }
    ]
    
    # Sample magazines
    sample_magazines = [
        {
            "uri": "http://example.com/magazine/1",
            "name": "週刊少年ジャンプ",
            "publisher": "集英社",
            "start_date": "1968",
            "end_date": ""
        },
        {
            "uri": "http://example.com/magazine/2", 
            "name": "週刊少年マガジン",
            "publisher": "講談社",
            "start_date": "1959",
            "end_date": ""
        },
        {
            "uri": "http://example.com/magazine/3",
            "name": "別冊少年マガジン",
            "publisher": "講談社",
            "start_date": "2009",
            "end_date": ""
        }
    ]
    
    # Create data directory if it doesn't exist
    if not os.path.exists('../data'):
        os.makedirs('../data')
    
    # Save sample data
    with open('../data/manga_works.json', 'w', encoding='utf-8') as f:
        json.dump(sample_works, f, ensure_ascii=False, indent=2)
    
    with open('../data/manga_authors.json', 'w', encoding='utf-8') as f:
        json.dump(sample_authors, f, ensure_ascii=False, indent=2)
    
    with open('../data/manga_magazines.json', 'w', encoding='utf-8') as f:
        json.dump(sample_magazines, f, ensure_ascii=False, indent=2)
    
    print("Sample data created successfully!")
    print(f"Works: {len(sample_works)}")
    print(f"Authors: {len(sample_authors)}")
    print(f"Magazines: {len(sample_magazines)}")

if __name__ == "__main__":
    create_sample_data()