import { Component, EventEmitter, Input, Output } from '@angular/core';

@Component({
  selector: 'app-success-dialog',
  template: `
    <div class="text-center py-5">
      <div class="mb-4">
        <i class="bi bi-check-circle-fill text-success" style="font-size: 5rem;"></i>
      </div>
      <h2 class="mb-3">Verification Successful</h2>
      <p class="mb-4">Your identity has been verified as a {{ userRole }}. You can now access the platform.</p>
      <div class="d-grid mb-3">
        <button type="button" class="btn btn-primary btn-lg" (click)="continue.emit()">
          Continue to TacticSense
        </button>
      </div>
      <div class="text-center mt-2">
        <button type="button" class="btn btn-link" (click)="signIn.emit()">
          Already have an account? Sign In
        </button>
      </div>
    </div>
  `,
  styles: []
})
export class SuccessDialogComponent {
  @Input() userRole: string = '';
  @Output() continue = new EventEmitter<void>();
  @Output() signIn = new EventEmitter<void>();
  @Output() close = new EventEmitter<void>();
}