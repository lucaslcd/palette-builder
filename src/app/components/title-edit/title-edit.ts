import { Component, input, signal } from '@angular/core';
import { Field, FormField } from '@angular/forms/signals';
import { MatIconButton } from '@angular/material/button';
import { MatIcon } from '@angular/material/icon';
import { MatFormField, MatInputModule } from '@angular/material/input';

@Component({
  selector: 'pbu-title-edit',
  imports: [FormField, MatFormField, MatIconButton, MatIcon, MatInputModule],
  templateUrl: './title-edit.html',
  styleUrl: './title-edit.css',
})
export class TitleEdit {
  formField = input.required<Field<string>>();
  isEditing = signal(false)

  edit() {
    this.isEditing.set(!this.isEditing())
  }
}

