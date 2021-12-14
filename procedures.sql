DROP PROCEDURE IF EXISTS deposit;
DELIMITER $$
CREATE PROCEDURE deposit(IN bid INT, IN tid INT, IN aid INT, IN dlt INT, IN cmt CHAR(30), OUT blnc INT)
BEGIN
	UPDATE branches SET balance = balance + dlt WHERE branchid = bid;
	UPDATE tellers SET balance = balance + dlt WHERE tellerid = tid;
	UPDATE accounts SET balance = balance + dlt WHERE accid = aid;
    SELECT balance INTO blnc FROM accounts WHERE accid = aid;
	INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES(aid, tid, dlt, bid, blnc, cmt);
END$$
DELIMITER ;