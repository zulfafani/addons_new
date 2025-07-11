// /** @odoo-module **/

// import { patch } from "@web/core/utils/patch";
// import { ProductsWidget } from "@point_of_sale/app/screens/product_screen/product_list/product_list";
// import { useService } from "@web/core/utils/hooks";
// import { ErrorPopup } from "@point_of_sale/app/errors/popups/error_popup";

// patch(ProductsWidget.prototype, {
//     setup() {
//         super.setup();
//         this.orm = useService("orm");
//         this.pos = useService("pos");
//         this.popup = useService("popup");
//         this.notification = useService("pos_notification");
//     },

//     async onProductClick(product) {
//         const employeeId = this.pos.get_cashier()?.employee_id?.[0];  // assuming employee_id is a tuple [id, name]
        
//         if (!employeeId) {
//             // Notify if employee data is missing
//             this.popup.add(ErrorPopup, {
//                 title: _t("Employee not found"),
//                 body: _t("The current employee is not valid, action cannot be performed."),
//             });
//             return;
//         }

//         try {
//             // Fetch employee data using ORM
//             const employeeData = await this.orm.searchRead(
//                 "hr.employee",
//                 [["id", "=", employeeId]],
//                 ["is_pic"]
//             );

//             const isPIC = employeeData?.[0]?.is_pic;

//             if (isPIC) {
//                 // Show warning if employee is a PIC (person in charge)
//                 this.notification.add("You are not allowed to select products as PIC.", {
//                     type: "warning",
//                 });
//                 return;  // Prevent product selection
//             }

//             // If the employee is not PIC, proceed with selecting the product
//             this.props.onProductClick?.(product);

//         } catch (error) {
//             console.error("Error fetching employee data:", error);
//             this.popup.add(ErrorPopup, {
//                 title: _t("Error"),
//                 body: _t("An error occurred while verifying employee data."),
//             });
//         }
//     }
// });
