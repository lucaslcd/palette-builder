import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ColorPointer } from './color-pointer';

describe('ColorPointer', () => {
  let component: ColorPointer;
  let fixture: ComponentFixture<ColorPointer>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ColorPointer],
    }).compileComponents();

    fixture = TestBed.createComponent(ColorPointer);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
