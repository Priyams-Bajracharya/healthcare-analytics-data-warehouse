INSERT INTO staging.claims (id, patientid, providerid, diagnosis1, diagnosis2, diagnosis3, diagnosis4, diagnosis5, diagnosis6, diagnosis7, diagnosis8, appointmentid)
SELECT r.id, r.patientid, r.providerid, r.diagnosis1, r.diagnosis2, r.diagnosis3, r.diagnosis4, r.diagnosis5, r.diagnosis6, r.diagnosis7, r.diagnosis8, r.appointmentid
FROM raw.claims r
INNER JOIN staging.encounters e ON r.appointmentid = e.id;