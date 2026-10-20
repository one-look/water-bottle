'''Errors raised while submitting attendance.'''


class AttendanceError(Exception):
    '''
    Base error for attendance submission.
    '''


class InvalidAttendance(AttendanceError):
    '''
    Raised when the submission payload is not acceptable.
    '''


class UnknownStudents(AttendanceError):
    '''
    Raised when a register number is not a student in the attendance database.
    '''


class TeacherNotRegistered(AttendanceError):
    '''
    Raised when the authenticated email is not a teacher.
    '''


class DuplicateAttendance(AttendanceError):
    '''
    Raised when attendance for a student, date, and period already exists.
    '''


class AttendancePersistenceError(AttendanceError):
    '''
    Raised when the database rejects the submission for another reason.
    '''
