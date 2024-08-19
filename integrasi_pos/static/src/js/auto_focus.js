// /** @odoo-module **/

// import { Component, useRef, onMounted, onWillUpdateProps } from "@odoo/owl";
// import { registry } from "@web/core/registry";

// /**
//  * Barcode input field that automatically focuses and processes barcode scanner input
//  */
// class BarcodeFocusWidget extends Component {
//     static template = "InventoryStock.BarcodeFocusWidget";
    
//     setup() {
//         this.inputRef = useRef("input");
        
//         onMounted(() => this.focusInput());
//         onWillUpdateProps(() => setTimeout(() => this.focusInput(), 0));
//     }

//     focusInput() {
//         if (this.inputRef && this.inputRef.el) {
//             try {
//                 this.inputRef.el.focus();
//             } catch (error) {
//                 console.warn("Failed to focus input: ", error);
//             }
//         }
//     }

//     /**
//      * Handle the input event directly in the component
//      */
//     handleInput(ev) {
//         // Update the model with the new value
//         if (this.props.update) {
//             this.props.update(ev.target.value);
//         } else {
//             console.warn("No update function available on props");
//         }
//     }

//     /**
//      * Process barcode scanner input on Enter key
//      */
//     processBarcode(ev) {
//         if (ev.key === 'Enter') {
//             // Prevent the default form submission
//             ev.preventDefault();
            
//             // The key improvement: force OWL to save the input value to the model
//             if (this.props.update) {
//                 this.props.update(this.inputRef.el.value);
//             }
            
//             // Manually trigger a change event to ensure the onchange method runs
//             this.inputRef.el.dispatchEvent(new Event('change', { bubbles: true }));
            
//             // Wait a small moment for the onchange to process, then focus back
//             setTimeout(() => {
//                 if (this.inputRef.el) {
//                     this.inputRef.el.focus();
//                 }
//             }, 100);
//         }
//     }
// }

// // Register the barcode widget with proper props extraction
// registry.category("fields").add("barcode_focus_widget", {
//     component: BarcodeFocusWidget,
//     supportedTypes: ['char'],
//     extractProps: ({ attrs, field, record }) => {
//         return {
//             value: record ? record.data[field.name] || '' : '',
//             update: (value) => {
//                 if (record) {
//                     record.update({ [field.name]: value });
//                 }
//             },
//         };
//     },
// });