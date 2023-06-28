from sqlalchemy import func, desc, and_, distinct, select

from src.models import Teacher, Student, Discipline, Grade, Group, Enrollment
from src.db import session


from sqlalchemy.orm import join


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    :return:
    """
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_2(discipline_id):
    """""
    Найти студента с наивысшим средним баллом по определенному предмету.
    """""
    result = session.query(Student.fullname, Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
    .select_from(Grade).join(Student).join(Discipline).filter(Discipline.id==discipline_id).group_by(Student.id, Discipline.name)\
    .order_by(desc("avg_grade")).first()
    return result


def select_3(discipline_id):
    """
    Найти средний балл в группах по определенному предмету.
    """
    result = session.query(Discipline.name, Group.name, func.avg(Grade.grade)) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(Discipline.id == discipline_id) \
        .group_by(Discipline.name, Group.name) \
        .all()
        
    return result


def select_4(discipline_id):
    """
    Найти средний балл на потоке (по всей таблице оценок).
    """
    result = session.query(func.avg(Grade.grade)).select_from(Grade).filter(Grade.discipline_id == discipline_id).scalar()
    return result


def select_5(teacher_id):
    """
    Найти какие курсы читает определенный преподаватель.
    """
    teacher = session.query(Teacher).get(teacher_id)
    if teacher is None:
        return None
    courses = [discipline.name for discipline in teacher.disciplines]
    return courses



def select_6(group_id):
    """
    Найти список студентов в определенной группе.
    """
    group = session.query(Group).get(group_id)
    if group is None:
        return None
    
    students = [student.fullname for student in group.students]
    return students



def select_7(group_id, discipline_id):
    """
    Найти оценки студентов в отдельной группе по определенному предмету.
    """
    group = session.query(Group).get(group_id)
    if group is None:
        return None
    
    grades = session.query(Student.fullname, Grade.grade) \
        .select_from(Grade) \
        .join(Student) \
        .filter(Grade.discipline_id == discipline_id, Student.group_id == group_id) \
        .all()
    
    return grades
    
def select_8():
    """
    Найти средний балл, который ставит определенный преподаватель по своим предметам.
    """
    result = session.query(distinct(Teacher.fullname), func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade) \
        .join(Discipline)  \
        .join(Teacher) \
        .where(Teacher.id == 3).group_by(Teacher.fullname).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_9(student_id):
    """
    Найти список курсов, которые посещает определенный студент.
    """
    courses = session.query(Discipline.name, Student.fullname).select_from(Discipline).join(Grade).join(Student).filter(Student.id==student_id)\
    .group_by(Discipline.name, Student.fullname).all()
    return courses


def select_10(student_id, teacher_id):
    """
    Найти список курсов, которые определенному студенту читает определенный преподаватель.
    """
    student = session.query(Student).get(student_id)
    teacher = session.query(Teacher).get(teacher_id)
    if student is None or teacher is None:
        return None
    
    courses = session.query(Discipline.name).select_from(Discipline).join(Grade).join(Student).join(Teacher) \
        .filter(Student.id == student_id, Teacher.id == teacher_id).distinct().all()
    return courses


def select_11(teacher_id, student_id):
    """
    Найти средний балл, который определенный преподаватель ставит определенному студенту.
    """
    result = session.query(func.avg(Grade.grade)) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Teacher) \
        .filter(and_(Teacher.id == teacher_id, Student.id == student_id)) \
        .first() 
    
    return round(float(result[0]),2)


def select_12():
    """
    Оценки студентов в определенной группе по определенному предмету на последнем занятии.
    """
    group_id = 2
    dis_id = 2
    # Знаходимо останнє заняття
    subq = (select(Grade.date_of).join(Student).join(Group).where(
        and_(Grade.discipline_id == dis_id, Group.id == group_id)
    ).order_by(desc(Grade.date_of)).limit(1)).scalar_subquery()

    result = session.query(Student.fullname, Discipline.name, Group.name, Grade.grade, Grade.date_of) \
        .select_from(Grade) \
        .join(Student) \
        .join(Discipline) \
        .join(Group) \
        .filter(and_(Grade.discipline_id == dis_id, Group.id == group_id, Grade.date_of == subq)) \
        .order_by(desc(Grade.date_of)).all()
    return result



if __name__ == '__main__':
    #print(select_1())
    #print(select_2(1))
    #print(select_3(1))
    #print(select_4(2))
    #print(select_5(1))
    #print(select_6(2))
    #print(select_7(3, 2))
    #print(select_8())
    #print(select_9(1))
    #print(select_10(3, 4))
    print(select_11(1, 1))
    #print(select_12())