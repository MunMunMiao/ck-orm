CREATE ROLE IF NOT EXISTS `ck_orm_e2e_role_analyst`;
CREATE ROLE IF NOT EXISTS `ck_orm_e2e_role_auditor`;
CREATE USER IF NOT EXISTS `ck_orm_e2e_transport`
IDENTIFIED BY 'ck_orm_e2e_transport_password';
GRANT `ck_orm_e2e_role_analyst`, `ck_orm_e2e_role_auditor` TO `ck_orm_e2e_transport`;
