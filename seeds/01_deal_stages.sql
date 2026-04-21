INSERT INTO deal_stages (stage_name, stage_order, probability, is_closed, is_won) VALUES
('Prospecting',      1, 10.00, FALSE, FALSE),
('Qualification',   2, 20.00, FALSE, FALSE),
('Needs Analysis',  3, 35.00, FALSE, FALSE),
('Proposal',        4, 50.00, FALSE, FALSE),
('Negotiation',     5, 70.00, FALSE, FALSE),
('Closed Won',      6, 100.00, TRUE, TRUE),
('Closed Lost',     7, 0.00,  TRUE, FALSE);
