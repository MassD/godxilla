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
  val cancel_order : platform_t -> order_id:order_id_t -> order_id_t option * platform_t
    
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
		     holdings : vol_t String.Map.t;
		   }

  type platform_t = { orders : order_t Int.Map.t;
		      accounts : account_t Int.Map.t;
		      stocks : price_t String.Map.t;
		    }
      
  let start () = { orders = Int.Map.empty; accounts = Int.Map.empty; stocks = String.Map.empty; }
    
  let create_account acc_id = 
    { id = acc_id;
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
  let find_pending_order p order_id = 
    match Map.find p.orders order_id with
      | Some order when order.status = Pending -> Some order
      | _ -> None

  let find_account p acc_id = 
    match Map.find p.accounts acc_id with
      | Some acc -> p,acc
      | None -> let acc = create_account acc_id in {p with accounts = Map.add p.accounts ~key:acc_id ~data:acc}, acc 

  (* Update order, used in amend and cancel *)
  let update_order p ~order_id ~update = 
     match find_pending_order p order_id with
      | None -> p
      | Some order -> 
	let order' = { update order with timestamp = Unix.time(); } in
	{p with orders = Map.add p.orders ~key:order_id ~data:order'}

  (* Stock position *) 
  let find_stock_pos p stock =
    match Map.find p.stocks stock with
      | None -> 0.
      | Some pos -> pos

  let update_stock_pos p ~stock ~price =
    {p with stocks = Map.add p.stocks ~key:stock ~data:price}

  (* Deal match *)
  let find_orders p ~filter ~cmp =
    Map.to_alist p.orders 
      |> List.filter ~f:filter 
      |> List.map ~f:snd 
      |> List.sort ~cmp

  let find_buys p sell = 
    find_orders p ~filter:(fun (_, buy) -> buy.stock = sell.stock && buy.price >= sell.price && buy.status = Pending) ~cmp:(fun o1 o2 -> compare (-.o1.price, o1.timestamp) (-.o2.price, o2.timestamp))
	  
  let find_sells p buy = 
    find_orders p ~filter:(fun (_, sell) -> sell.stock = buy.stock && sell.price <=  buy.price && sell.status = Pending) ~cmp:(fun o1 o2 -> compare (o1.price, o1.timestamp) (o2.price, o2.timestamp))
      

  let update_account p ~acc_id ~stock ~vol =
    let p, acc = find_account p acc_id in
    let new_vol = 
      match Map.find acc.holdings stock with
      | None -> 0.
      | Some v -> v +. vol
    in 
    let acc = {acc with holdings = Map.add acc.holdings ~key:stock ~data:new_vol} in
    {p with accounts = Map.add p.accounts ~key:acc_id ~data:acc}

  let complete_order p ~order_id = update_order p ~order_id ~update:(fun order -> {order with status = Completed;})

  let try_sell p ~sell =
    if sell.order_type = Buy then assert false
    else 
      let buys = find_buys p sell in
      let rec loop p sell = function
	| [] -> p
	| buy::buys -> (
	  let pos = (buy.price +. sell.price) /. 2. in
	  let leftover_vol = Float.abs (buy.vol -. sell.vol) in 
	  if buy.vol > sell.vol then 
	    complete_order p ~order_id:sell.id 
			   |> update_account ~acc_id:buy.id ~stock:buy.stock ~vol:(sell.vol) 
			   |> update_account ~acc_id:sell.id ~stock:sell.stock ~vol:(-.sell.vol) 
			   |> update_order ~order_id:buy.id ~update:(fun buy_order -> {buy_order with vol = leftover_vol}) 
			   |> update_stock_pos ~stock:sell.stock ~price:pos
	  else if buy.vol = sell.vol then
	    complete_order p ~order_id:sell.id 
			   |> complete_order ~order_id:buy.id 
			   |> update_account ~acc_id:buy.id ~stock:buy.stock ~vol:(sell.vol) 
			   |> update_account ~acc_id:sell.id ~stock:sell.stock ~vol:(-.sell.vol)
			   |> update_stock_pos ~stock:sell.stock ~price:pos
	  else 
	    let p = complete_order p ~order_id:buy.id 
			   |> update_account ~acc_id:buy.id ~stock:buy.stock ~vol:(buy.vol) 
			   |> update_account ~acc_id:sell.id ~stock:sell.stock ~vol:(-.buy.vol)
			   |> update_order ~order_id:sell.id ~update:(fun sell_order -> {sell_order with vol = leftover_vol}) 
			   |> update_stock_pos ~stock:sell.stock ~price:pos
	    in 
	    loop p {sell with vol = leftover_vol} buys 
	)
      in 
      loop p sell buys
 
  let try_buy p ~buy =
    if buy.order_type = Sell then assert false
    else 
      let sells = find_sells p buy in
      let rec loop p buy = function
	| [] -> p
	| sell::sells -> (
	  let pos = (buy.price +. sell.price) /. 2. in
	  let leftover_vol = Float.abs (buy.vol -. sell.vol) in 
	  if buy.vol < sell.vol then 
	    complete_order p ~order_id:buy.id 
			   |> update_account ~acc_id:buy.id ~stock:buy.stock ~vol:buy.vol
			   |> update_account ~acc_id:sell.id ~stock:sell.stock ~vol:(-.buy.vol)
			   |> update_order ~order_id:sell.id ~update:(fun sell_order -> {sell_order with vol = leftover_vol}) 
			   |> update_stock_pos ~stock:sell.stock ~price:pos
	  else if buy.vol = sell.vol then
	    complete_order p ~order_id:buy.id 
			   |> complete_order ~order_id:sell.id 
			   |> update_account ~acc_id:buy.id ~stock:buy.stock ~vol:buy.vol
			   |> update_account ~acc_id:sell.id ~stock:sell.stock ~vol:(-.sell.vol) 
			   |> update_stock_pos ~stock:sell.stock ~price:pos
	  else 
	    let p = complete_order p ~order_id:sell.id 
			   |> update_account ~acc_id:buy.id ~stock:buy.stock ~vol:(sell.vol) 
			   |> update_account ~acc_id:sell.id ~stock:sell.stock ~vol:(-.sell.vol)
			   |> update_order ~order_id:buy.id ~update:(fun buy_order -> {buy_order with vol = leftover_vol}) 
			   |> update_stock_pos ~stock:sell.stock ~price:pos
	    in 
	    loop p {buy with vol = leftover_vol} sells
	)
      in 
      loop p buy sells
      
  (* New order *)
  let new_order p ~acc_id ~stock ~order_type ~price ~vol = 
    let order = create_order ~acc_id ~stock ~order_type ~price ~vol in
    let p = {p with orders = Map.add p.orders ~key:order.id ~data:order} in
    match order_type with
      | Buy -> Some order.id, try_buy p ~buy:order
      | Sell -> Some order.id, try_sell p ~sell:order
    
  (* Amend order *)
  let amend_order p ~order_id ~price ~vol = 
    let p = update_order p ~order_id ~update:(fun order -> {order with price = price; vol = vol;}) in
    match find_pending_order p order_id with
      | None -> assert false
      | Some order -> Some order_id, (
	match order.order_type with
	  | Buy -> try_buy p ~buy:order
	  | Sell -> try_sell p ~sell:order
      )
     
  (* Cancel order *)
  let cancel_order p ~order_id = None, update_order p ~order_id ~update:(fun order -> {order with status = Cancelled;})

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
