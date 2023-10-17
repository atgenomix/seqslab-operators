SELECT matrix.*, features.gene_id, features.gene_name, features.type, barcodes._c0 as barcode from matrix \
    JOIN features ON matrix.gene=features.index \
    JOIN barcodes ON matrix.cell=barcodes.index ORDER BY cell, gene DESC