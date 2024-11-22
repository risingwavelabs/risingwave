-- Populate supplier (insert 10,000 rows)
UPDATE supplier SELECT
                         1 AS s_suppkey,
                         'Supplier#000000001' AS s_name,
                         '123456' AS s_address,
                         1 AS s_nationkey,
                         '8888888' AS s_phone,
                         1000 AS s_acctbal,
                         'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer suscipit, lacus eget egestas viverra, leo diam efficitur ante, ac congue libero ligula ut libero. Morbi sed enim magna. Sed ac facilisis libero, sit amet tincidunt erat. Fusce semper libero quis venenatis gravida. Fusce consequat enim nec orci ultrices gravida. Mauris mattis sem non molestie accumsan. Praesent nibh metus, volutpat sit amet commodo ut, faucibus ut dolor. Sed tincidunt ante vitae finibus commodo. Duis velit tellus, porta non tortor id, mattis aliquam lectus. Vestibulum quis posuere justo, non rutrum nibh. Sed ultricies ac est ac ullamcorper. Nulla et ipsum sed nisl laoreet tristique ac sed eros. Cras pellentesque posuere massa non auctor. Ut ipsum lorem, laoreet venenatis vehicula et, scelerisque a felis. Fusce ornare leo nec varius eleifend.' AS s_comment
FROM generate_series(1, 1);
