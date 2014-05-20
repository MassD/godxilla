#require "str"

exception Invalid_order_type of string
exception Invalid_event

type account_id_t = int
type stock_t = string
type price_t = float
type vol_t = float

type order_type_t =
  | Buy
  | Sell

type order_id_t = int

let order_type_of_string s = 
  match String.lowercase s with
    | "buy" -> Buy
    | "sell" -> Sell
    | _ -> raise (Invalid_order_type s)

module Id_generator :sig 
  val next : unit -> int
end = struct
  let ivar = ref 0 
  let next () = ivar := !ivar+1;!ivar
end

module type Platform = sig
  exception Internal_err of string
  type platform_t
  
  val start : unit -> platform_t
  val new_order : platform_t -> acc_id:account_id_t -> stock:stock_t -> order_type:order_type_t -> price:price_t -> vol:vol_t -> order_id_t option * platform_t
  val amend_order : platform_t -> order_id:order_id_t -> price:price_t -> vol:vol_t -> order_id_t option * platform_t
  val cancel_order : platform_t -> order_id:order_id_t -> orderid_t option * platform_t
    
  (*val all_stocks : platform_t -> stock_t list
  val query_stock : platform_t -> stock_t -> price_t
  val query_account_deal : platform_t -> account_t -> (stock_t * price_t * vol_t) list
  val query_account_pending : platform_t -> account_t -> (order_type_t * stock_t * price_t * vol_t) list*)
end

module Godxilla : Platform = struct
  exception Internal_err of string

  type order_status_t = Completed | Pending | Cancelled

  type order_t = { id : order_id_t;
		   acc_id : account_id_t;
		   stock : stock_t;
		   order_type : order_type_t;
		   price : price_t;
		   vol : vol_t;
		   status : order_status_t;
		   timestamp : float;
		 }

  type account_t = { id : account_id_t;
		     orders : order_t Int.Map.t;
		     holdings : vol_t String.Map.t;
		   }

  type platform_t = { buy : order_t Int.Map.t;
		      sell : order_t Int.Map.t;
		      accounts : account_t Int.Map.t;
		      stocks : price_t String.Map.t;
		    }
      
  let start () = { buy = Int.Map.empty; sell = Int.Map.empty; accounts = Int.Map.empty; stocks = String.Map.empty; }
    
  let create_account acc_id = 
    { id = acc_id;
      orders = Int.Map.empty;
      holdings = String.Map.empty;
    }
    
  let create_order ~acc_id ~stock ~order_type ~price ~vol =
    { id = Id_generator.next();
      acc_id;
      stock;
      order_type;
      price;
      vol;
      status = Pending;
      timestamp = Unix.time()
    }

(* Utilities *)
  let find_order buy sell order_id = 
    match Map.find buy order_id, Map.find sell order_id with
      | Some _, Some _ -> raise (Internal_err "Same key existing in both buy and sell")
      | Some order, None when order.status = Pending -> Some order, Some Buy
      | None, Some order when order.status = Pending -> Some order, Some Sell
      | _ -> None, None

  let find_account accounts acc_id = Map.find accounts acc_id

  let update_order ({buy;sell;accounts;stocks} as p) ~order_id ~update ~update_accounts = 
    match find_order buy sell order_id with
      | None, _ | _, None -> None, p
      | Some order, Some Buy -> None, {p with buy = update buy order; accounts = update_accounts accounts order}
      | Some order, Some Sell -> None, {p with sell = update sell order; accounts = update_accounts accounts order}

(* Deal match *)
  let find_stock_orders orders stock =
    let filter = fun (_, order) -> order.stock = stock && order.status = Pending in
    let order_list = Map.to_alist orders |> List.filter ~f:filter in
    order_list

  let sort_buy bar ol = 
    List.sort ~cmp:(
      fun (_, o1) (_, o2) -> compare o1.timestamp o2.timestamp) ol
  let sort_sell bar ol = 
    List.sort ~cmp:(
      fun (_, o1) (_, o2) -> compare (Float.abs(o1.price-.bar), o1.timestamp) (Float.abs(o2.price-.bar),o2.timestamp)) ol
      

  let find_stock_pos p stock =
    match Map.find p.stocks stock with
      | None -> 0.
      | Some pos -> pos

  let update_stock_pos p stock price =
    Map.add p.stocks ~key:stock ~data:price
      
  
      

(* New order *)
  let new_order ({buy;sell;accounts} as p) ~acc_id ~stock ~order_type ~price ~vol = 
    let order = create_order ~acc_id ~stock ~order_type ~price ~vol in
    let acc = 
      match find_account accounts acc_id with
	| None -> let new_acc = create_account acc_id in {new_acc with orders = Map.add new_acc.orders ~key:order.id ~data:order}
	| Some acc -> {acc with orders = Map.add acc.orders ~key:order.id ~data:order}
    in 
    match order_type with
      | Buy -> Some order.id, {p with buy = Map.add buy ~key:order.id ~data:order; accounts = Map.add accounts ~key:acc.id ~data:acc}
      | Sell -> Some order.id, {p with sell = Map.add sell ~key:order.id ~data:order; accounts = Map.add accounts ~key:acc.id ~data:acc}

(* Amend order *)
  let amend price vol side_map order =
    let amended_order = {order with price = price; vol = vol; timestamp = Unix.time();} in
    Map.add side_map ~key:order.id ~data:amended_order

  let amend_in_accounts price vol accounts order =
    match find_account accounts order.acc_id with
      | None -> raise (Internal_err "Cannot find order in accounts")
      | Some acc -> Map.add accounts ~key:order.acc_id ~data:{acc with orders = amend price vol acc.orders order}

  let amend_order p ~order_id ~price ~vol = 
    update_order p ~order_id ~update:(amend price vol) ~update_accounts:(amend_in_accounts price vol)

(* Cancel order *)
  let cancel side_map order =
    let cancelled_order = {order with status = Cancelled; timestamp = Unix.time();} in
    Map.add side_map ~key:order.id ~data:cancelled_order

  let cancel_in_accounts accounts order =
    match find_account accounts order.acc_id with
      | None -> raise (Internal_err "Cannot find order in accounts")
      | Some acc -> Map.add accounts ~key:order.acc_id ~data:{acc with orders = cancel acc.orders order}

  let cancel_order p ~order_id = 
    update_order p ~order_id ~update:cancel ~update_accounts:cancel_in_accounts
end



type event_t =
    | New of account_id_t * stock_t * order_type_t * price_t * vol_t
    | Amend of order_id_t * price_t * vol_t
    | Cancel of order_id_t

module Event_parser : sig 
  val parse_an_event : string -> event_t
end = struct
  let create_new acc_id stock order_type price vol =
    New (int_of_string acc_id, stock, order_type, Float.of_string price, Float.of_string vol)
  let create_amend order_id price vol =
    Amend (int_of_string order_id, Float.of_string price, Float.of_string vol)
  let create_cancel order_id = Cancel (int_of_string order_id)

  let create_event = function
    | [acc_id; stock; order_type; "new"; price; vol] -> create_new acc_id stock (order_type_of_string order_type) price vol
    | [_; _; _; "amend"; price; vol; order_id] -> create_amend order_id price vol
    | [_; _; _; "cancel"; order_id] -> create_cancel order_id
    | _ -> raise Invalid_event

  let parse_an_event s =
    let splitter = Str.regexp "\t" in
    let sl = Str.split splitter s |> List.map ~f:String.lowercase in
    try create_event sl with _ -> raise Invalid_event
end



module Event_handler (P:Platform) = struct

  let init () = P.start ()

  let process p = function
    | New (acc_id, stock, order_type, price, vol) -> P.new_order p ~acc_id ~stock ~order_type ~price ~vol
    | Amend (order_id, price, vol) -> P.amend_order p ~order_id ~price ~vol
    | Cancel order_id -> P.cancel_order p ~order_id
end
