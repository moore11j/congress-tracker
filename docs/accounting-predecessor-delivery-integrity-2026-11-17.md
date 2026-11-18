# Accounting predecessor original delivery integrity

The three reviewed VMRK and AvalonBay originals have two retained delivery variants. Their only byte differences are the terminal SEC delivery-script URL. The existing `sec_terminal_delivery_script_path_v1` policy confirms that all other bytes, including financial text, contexts, tags, units and whitespace, are identical.

The accounting predecessor reader now uses that existing integrity policy. It retains the received SHA256 separately from the reviewed SHA256 and the document digest. Parsed facts retain the actual received source hash. Cache validation requires the matching received-source receipts. Changed financial content, nonterminal scripts, script attributes, unknown source documents and future filings remain held.

Acquisition errors retain their specific reason through projection. VMRK alone receives a new preparation version so a previous failed preparation does not suppress a new attempt. No existing cooldown is shortened. Complete-source tests cover both delivery sets, cache replacement and a transport-free cached repeat, as well as alterations outside the permitted terminal URL.
